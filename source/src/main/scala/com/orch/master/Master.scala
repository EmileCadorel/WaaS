package com.orch.master

import akka.remote.DisassociatedEvent
import java.io.{File}
import scala.Array
import akka.actor.{Props, Actor, ActorSystem, ActorRef, ActorLogging , ActorSelection, PoisonPill }
import com.typesafe.config.ConfigFactory

import util.control.Breaks._

import akka.util.ByteString
import java.net.InetSocketAddress
import java.io._
import java.util.{Map => JMap, List => JArray}
import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConverters._
import com.orch.common._
import com.orch.daemon._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * The master class is that class that manage the nodes of one cluster
  * It is the entry point of new submissions
  */
class Master (name : String, LAddr : String, LPort : Int, EAddr : String, EPort : Int, LBW : Int, SCHED : String) extends Actor with ActorLogging {

    import com.orch.utils.{Path}
    import com.orch.master.MasterProto._
    import com.orch.leader.configuration._

    /**
      * The full graph of communication
      */
    var peerModules : Map [String, ActorSelection] = Map ()
    var peerModulesInfos : Map [String, (String, Int)] = Map ()
    var leader : ActorSelection = null
    
    /**
      * The daemons managed by this module 
      */
    var daemonNodes : Map [String, (ActorSelection, Node)] = Map ()

    /**
      * 
      */
    var daemonNodeInfos : Map [String, (String, Int)] = Map ()


    /**
      * The configuration is the state of what needs to be run copied and so on, inside the cluster
      */
    var configuration : Configuration = new Configuration ()

    var zeroInstant : Long = System.currentTimeMillis () / 1000;


    override def preStart () : Unit = {
        peerModulesInfos = peerModulesInfos + (name -> (LAddr, LPort))
        peerModules = peerModules + (name -> context.actorSelection (s"akka.tcp://RemoteSystem@$LAddr:$LPort/user/master"))
        this.leader = context.actorSelection (s"akka.tcp://RemoteSystem@$EAddr:$EPort/user/leader")

        context.system.eventStream.subscribe(self, classOf[DisassociatedEvent]);

        // Temporary
        Global.flow_home = "/tmp/" + name + "/" 

        log.info ("Starting Http file Server...")
        FileIO.startServer (Global.flow_home, LPort + 200);

        log.info ("Starting IPerf Server ...")
        IPerf.startServer (LPort + 100)

        log.info ("Starting network discovery...")
        this.leader ! MasterProto.AskForWelcome (name, LAddr, LPort, LBW)
    }

    def receive : Receive = {
        /**
          * ===============================================
          *               Daemon Connections
          * ===============================================
          */

        case DaemonProto.InformMaster (addr, port, node, boots) =>
            log.info (s"New daemon ${node.n_id} is connected")
            val ac = context.actorSelection (s"akka.tcp://RemoteSystem@${addr}:${port}/user/daemon")
            daemonNodes += (node.n_id -> (ac, node))
            daemonNodeInfos += (node.n_id -> (addr, port))
            this.leader ! NewNode (this.name, node, boots)

        /**
          * ===============================================
          *               Peer Messages
          * ===============================================
          */

        case Welcome (topology : Map [String, (String, Int)], _ : String, _ : Int, zero : Long) =>
            log.info ("Leader has welcomed me !")
            var bw : Map [String, Int] = Map ()
            for (it <- topology) {
                peerModulesInfos = peerModulesInfos + (it._1 -> (it._2._1, it._2._2))
                val entry =  context.actorSelection (s"akka.tcp://RemoteSystem@${it._2._1}:${it._2._2}/user/master")
                peerModules = peerModules + (it._1 -> entry)
                entry ! PeerHandShake (name, LAddr, LPort)
                bw = bw + (it._1-> IPerf.launchPerf (it._2._1, it._2._2 + 100))
            }

            // We don't change the right element of the left, because it is the leader, and he already performed the change
            for (it <- bw)
                this.leader ! BwNetwork (this.name, it._1, it._2)

            this.zeroInstant = zero
            this.startPlaying ();


        case PeerHandShake (name, ip, port) =>
            log.info (s"Discoverd new peer : ${name} at ${ip}:${port}")
            val entry = context.actorSelection (s"akka.tcp://RemoteSystem@${ip}:${port}/user/master")
            peerModules = peerModules + (name -> entry)
            peerModulesInfos = peerModulesInfos + (name -> (ip, port))


        /**
          * ===============================================
          *               Leader Election
          * ===============================================
          */


        case DisassociatedEvent (local, remote, inBound) =>
            val result = peerModulesInfos.find ((t) =>
                remote.hostPort.indexOf (t._2._1 + ":" + t._2._2) != -1
            );

            result match {
                case Some (x) =>
                    // Operator .- !?
                    peerModules = peerModules .- (x._1)
                    peerModulesInfos = peerModulesInfos .- (x._1)
                    log.info (s"Disassociation of master ${x._1}")
                    log.error ("We lost someone, no rescue plan, system halting !!")
                    self ! PoisonPill
                    context.system.terminate ();
                    System.exit (0);
                case _ =>
                    // Do nothing, but without this case the program crash
            }

        /**
          *  ===========================================
          *           Configuration Update 
          *  ===========================================
          */
        case UpdateConfiguration (vms, tasks, files) =>
            log.info ("Received new configuration");
            val prev_min_time = this.configuration.computeMinTime ()
            this.configuration.synchronized {
                for (v <- vms)
                    this.configuration.insertOrUpdate (v._2)

                var toDownloadFile : Map [String, CFile] = Map ()
                var toSend : Map [String, CFile] = Map ()
                for (f <- files) {// We start by the file so we don't need to go through the task twice
                    this.configuration.insertOrUpdate (f._1, f._2)
                    for (file <- f._2) {
                        if (file.io == CFileType.INPUT && file.cluster != this.name) {
                            toDownloadFile = toDownloadFile + (file.name-> file)
                        } else if (file.io == CFileType.INPUT) {
                            toSend = toSend + (file.name-> file)
                        }
                    }
                }

                var toDownloadExec : Map [String, (CTask, Array [Long])] = Map ()
                for (t <- tasks) {
                    this.configuration.insertOrUpdate (t._2)
                    // We need to retreive the tasks executable
                    if (t._2.cluster == this.name) { // Already there
                        this.configuration.executableFound (t._2.wid + "/" + t._2.id, Path.buildStr (Seq ("/", t._2.wid, t._2.app)).file)
                    } else {
                        val (ctask, array) : (CTask, Array [Long]) = if (toDownloadExec.contains (t._2.app)) {
                            toDownloadExec (t._2.app)
                        } else (t._2, Array ())
                        toDownloadExec = toDownloadExec + (t._2.app-> (ctask, array :+ t._2.id))
                    }
                }

                for (x <- toDownloadFile) {
                    val remote = peerModulesInfos (x._2.cluster)
                    // Http server is on port LPort + 200
                    FileIO.recvInput (self, remote._1, remote._2 + 200, x._2.wid, x._2.name, x._2.path)
                }

                for (x <- toDownloadExec) {
                    val remote = peerModulesInfos (x._2._1.cluster)
                    // Http server is on port LPort + 200
                    FileIO.recvExec (self, remote._1, remote._2 + 200, x._2._1.wid, x._2._2, x._2._1.app)
                }

                for (x <- toSend) {
                    self ! InputReceived (x._2.wid, x._2.name)
                }
            }

        case PlayConfiguration () =>
            this.configuration.synchronized {
                val min_start = this.configuration.computeMinTime ()
                println ("CONFIGURATION : " + this.configuration.getVMsByStart + " " + min_start + " " + getCurrentTime );
                if (min_start <=  getCurrentTime && min_start != -1) {
                    log.info ("Playing configuration")
                    // Start the VMs that needs to be started
                    launchVMs ()

                    // Start the tasks that needs to be started
                    launchTasks ()
                }
            }

            
        /**
          *  ===========================================
          *           Configuration Running
          *  ===========================================
          */            
        case ExecutableReceived (wid, app, ids) =>
            log.info (s"Downloaded executable $wid/$app")
            for (tid <- ids) {
                this.configuration.executableFound (wid + "/" + tid, app)
                val files = this.configuration.getFilesByEntry (wid + "/" + tid)
                val task = this.configuration.getTaskById (wid, tid)
                val vm = this.configuration.getVMById (task.vid)

                if (vm.state == CVMState.RUNNING && files.length == 0 && task.state == CTaskState.HAS_EXEC) {
                    log.info (s"${task.wid}/${task.id}:${task.app} is ready to launch at ${task.start}")
                    this.configuration.insertLaunchable (task)
                } else if (files.length == 0 && task.state == CTaskState.HAS_EXEC) {
                    log.info (s"${task.wid}/${task.id}:${task.app} Would be launchable if its vm was ready")
                }
            }

        case InputReceived (wid, fid) =>
            log.info (s"Input received : ${wid}/${fid}")
            val fList = this.configuration.getFilesById (wid + "/" + fid)
            var toRecv : Map [String, (CFile, String, Set [Long])] = Map ()
            var user = "";
            for (f <- fList) {
                val task = this.configuration.getTaskById (f.wid, f.deps)
                val vm = this.configuration.getVMById (task.vid)
                val array : (CFile, String, Set [Long]) = if (toRecv.contains (f.name + "@" + vm.nid))
                    toRecv (f.name + "@" + vm.nid)
                else (f, vm.nid, Set ())

                toRecv = toRecv + ((f.name + "@" + vm.nid)-> (f, vm.nid, array._3 + task.id))
                user = vm.user;
            }

            for (f <- toRecv) {
                val file = f._2._1
                val nid = f._2._2
                val tasks = f._2._3.toArray                

                // From the outside, the flow_home is the root dir
                val daemon = this.daemonNodes (nid)
                val path = Path.buildStr (Seq ("/", wid, "input", file.path)).file
                daemon._1 ! DaemonProto.RecvFile (nid, wid, file.name, LAddr, LPort + 200, path, user, tasks)
            }

        case TransfertReceived (wid, fid) =>
            log.info (s"Transfert received : ${wid}/${fid}")
            val fList = this.configuration.getFilesById (wid + "/" + fid)
            var toRecv : Map [String, (CFile, String, Set [Long])] = Map ()
            var user = "";
            for (f <- fList) {
                val task = this.configuration.getTaskById (f.wid, f.deps)
                val vm = this.configuration.getVMById (task.vid)

                val array : (CFile, String, Set [Long]) = if (toRecv.contains (f.name + "@" + vm.nid))
                    toRecv (f.name + "@" + vm.nid)
                else (f, vm.nid, Set ())

                toRecv = toRecv + ((f.name + "@" + vm.nid)-> (f, vm.nid, array._3 + task.id))
                user = vm.user;
            }

            for (f <- toRecv) {
                val file = f._2._1
                val nid = f._2._2
                val tasks = f._2._3.toArray

                // From the outside, the flow_home is the root dir
                val daemon = this.daemonNodes (nid)
                val path = Path.buildStr (Seq ("/", wid, "trans", file.path)).file
                daemon._1 ! DaemonProto.RecvFile (nid, wid, file.name, LAddr, LPort + 200, path, user, tasks)
            }

        case OutputReceived (wid, fid) =>
            this.leader ! OutputReceived (wid, fid)

        case DaemonProto.FileSent (nid, wid, fid, tids) =>
            for (tid <- tids) {
                log.info (s"File ${fid} sent on ${nid} for ${tid}")
                // Remove the dependencies
                val fList = this.configuration.getFilesById (wid + "/" + fid).filter ((x) => x.deps != tid)
                val files = this.configuration.getFilesByEntry (wid + "/" + tid).filter ((x) => x.name != fid)
                this.configuration.updateFileEntry (wid + "/" + tid, files)
                this.configuration.updateFileById (wid + "/" + fid, fList)

                // Check if the task is launchable
                val task = this.configuration.getTaskById (wid, tid)
                val vm = this.configuration.getVMById (task.vid)

                if (vm.state == CVMState.RUNNING && files.length == 0 && task.state == CTaskState.HAS_EXEC) {
                    log.info (s"${task.wid}/${task.id}:${task.app} is ready to launch at ${task.start}")
                    this.configuration.insertLaunchable (task)
                } else if (files.length == 0 && task.state == CTaskState.HAS_EXEC) {
                    log.info (s"${task.wid}/${task.id}:${task.app} Would be launchable if its vm was ready")
                }
            }
            
        case DaemonProto.VmReady (nid, vid, log_) =>
            log.info (s"VM ${nid}/${vid} is Ready")
            this.configuration.runningVM (vid);
            val tasks = this.configuration.getTasksByVM (vid)
            for (tid <- tasks) {
                val task = this.configuration.getTaskById (tid);
                val files = this.configuration.getFilesByEntry (tid)
                if (files.length == 0 && task.state == CTaskState.HAS_EXEC) {
                    log.info (s"${task.wid}/${task.id}:${task.app} is ready to launch at ${task.start}")
                    this.configuration.insertLaunchable (task)
                }
            }

        case DaemonProto.VmOff (nid, vid, log_) =>
            log.info (s"VM ${nid}/${vid} is Off")
            this.configuration.killedVM (vid)

        case DaemonProto.TaskFailure (nid,wid,id,log)=>
            println ("Task failure : " + nid + "@" + wid + "/" + id + " " + log);
            self ! PoisonPill
            context.system.terminate ();
            System.exit (0);

        case DaemonProto.TaskEnd (nid, wid, tid) =>
            log.info (s"Task ${wid}/${tid}@${nid} finished")
            val root = this.daemonNodeInfos (nid)
            val task = this.configuration.getTaskById (wid + "/" + tid)
            val vm = this.configuration.getVMById (task.vid)
            this.configuration.endTask (wid + "/" + tid);

            var toTransfert : Map [String, (CFile, Set [String])] = Map ()
            var toDownload  : Map [String, (CFile, Set [String])] = Map ()
            var toRecv : Map [String, (CFile, String, String, Set [Long])] = Map ()

            for (fid <- task.outs) {
                val files = this.configuration.getFilesById (wid + "/" + fid);
                for (f <- files) {
                    if (f.cluster == this.name && f.deps != -1) {
                        val depTask = this.configuration.getTaskById (wid + "/" + f.deps);
                        val depVM = this.configuration.getVMById (depTask.vid);
                        val path = Path.buildStr (Seq ("/", vm.user, wid, "" + task.id, f.path)).file;
                        val array : (CFile, String, String, Set [Long]) = if (toRecv.contains (f.name + "@" + depVM.nid))
                            toRecv (f.name + "@" + depVM.nid)
                        else (f, depVM.nid, path, Set ())
                        toRecv = toRecv + ((f.name + "@" + depVM.nid) -> (f, depVM.nid, path, array._4 + f.deps))
                    } else if (f.cluster != this.name && f.io != CFileType.OUTPUT) {
                        var array : (CFile, Set [String]) = if (toTransfert.contains (f.name))
                            toTransfert (f.name)
                        else (f, Set ())

                        toTransfert = toTransfert + (f.name -> (f, array._2 + f.cluster))
                    } else if (f.io == CFileType.OUTPUT) {
                        var array : (CFile, Set [String]) = if (toDownload.contains (f.name))
                            toDownload (f.name)
                        else (f, Set ())

                        toDownload = toDownload + (f.name -> (f, array._2 + f.cluster))
                    }
                }
            }

            for (f <- toRecv) {
                val file = f._2._1
                val nid = f._2._2
                val path = f._2._3
                val tasks = f._2._4.toArray

                val daemon = this.daemonNodes (nid);
                daemon._1 ! DaemonProto.RecvFile (nid, wid, file.name, root._1, root._2 + 200, path, vm.user, tasks)
            }

            for (f <- toTransfert) {
                val file = f._2._1;
                val clusters = f._2._2.toArray;
                val path = Path.buildStr (Seq ("/", vm.user, wid, "" + task.id, file.path)).file;
                FileIO.transfertForRemote (self, root._1, root._2 + 200, clusters, wid, file.name, path, file.path)
            }

            for (f <- toDownload) {
                val file = f._2._1;
                val clusters = f._2._2.toArray;
                val path = Path.buildStr (Seq ("/", vm.user, wid, "" + task.id, file.path)).file;
                FileIO.downloadForRemote (self, root._1, root._2 + 200, clusters, wid, file.name, path, file.path)
            }

            if (this.configuration.getTasksByVM (task.vid).length == 0) {
                val daemon = this.daemonNodes (nid)
                daemon._1 ! DaemonProto.KillVM (nid, task.vid)
            }

        case TransfertForRemote (clusters, wid, fid, path) =>
            log.info (s"Transfert ${wid}/${fid} from node for a remote cluster ${clusters.mkString (", ")}")
            for (c <- clusters) { // Cannot have this.name here
                val peer = this.peerModules (c)
                peer ! TransfertFromRemote (LAddr, LPort, wid, fid, path)
            }

        case TransfertFromRemote (addr, port, wid, fid, path) =>
            log.info (s"Transfering ${wid}/${fid}")
            FileIO.recvTrans (self, addr, port + 200, wid, fid, path)

        case DownloadForRemote (clusters, wid, fid, path) =>
            log.info (s"Download Result ${wid}/${fid} for remote ${clusters.mkString (", ")}")
            for (c <- clusters) {
                if (c != this.name) {
                    val peer = this.peerModules (c)
                    peer ! DownloadFromRemote  (LAddr, LPort, wid, fid, path)
                } else self ! OutputReceived (wid, fid);                
            }

        case DownloadFromRemote (addr, port, wid, fid, path) => 
            log.info (s"Download Result ${wid}/${fid}")
            FileIO.recvOutput (self, addr, port + 200, wid, fid, path)

        case x => {
            log.error ("Unknown message : " + x)
        }
    }


    def getCurrentTime () : Long = {
        return (System.currentTimeMillis () / 1000) - zeroInstant;
    }

    def launchVMs () : Unit = {
        val vmsByStart = this.configuration.getVMsByStart ()
        val starts = vmsByStart.keySet.toList.sortWith (_ < _)
        breakable {
            for (x <- starts) {
                if (x <= getCurrentTime) {
                    val vms = vmsByStart (x)
                    for (vid <- vms) {
                        val v = this.configuration.getVMById (vid);
                        this.configuration.bootingVM (v.id, x)
                        val daemon = this.daemonNodes (v.nid)
                        log.info (s"Launch VM : ${v.id}@${v.nid}")
                        daemon._1 ! DaemonProto.LaunchVM (v.nid, v.id, v.os, v.capas, v.user, "")
                    }
                } else break
            }
        }
    }


    def launchTasks () : Unit = {
        val tasksByStart = this.configuration.getTasksByStart ()
        val starts = tasksByStart.keySet.toList.sortWith (_ < _)
        breakable {
            for (x <- starts) {
                if (x <= this.getCurrentTime) {
                    val tasks = tasksByStart (x);
                    for (tid <- tasks) {
                        val t = this.configuration.getTaskById (tid);
                        this.configuration.startTask (t.wid + "/" + t.id, x)
                        val v = this.configuration.getVMById (t.vid)
                        val daemon = this.daemonNodes (v.nid)
                        log.info (s"Starting task : ${t.wid}/${t.id}:${t.app}@${v.nid} on ${t.vid}")
                        daemon._1 ! DaemonProto.RunTask (v.nid, t.wid, t.id, t.app, v.user, t.vid, t.params)
                    }
                }
            }
        }
    }
    
    def startPlaying () : Unit = {
        Future {
            while (true) {
                Thread.sleep (1000);
                self ! PlayConfiguration () // Sync is guaranteed by actor system
            }
        }
    }


}


