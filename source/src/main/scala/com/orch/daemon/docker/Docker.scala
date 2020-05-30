package com.orch.daemon.docker

import org.json4s._
import java.io._
import java.net.ConnectException
import com.orch.utils._
import java.util.{Map => JMap, List => JArray}
import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConverters._
import akka.actor.{ ActorSelection }
import scala.collection.mutable.Queue
import com.jcraft.jsch._;
import java.util.stream._
import util.control.Breaks._
import com.orch.daemon.docker.Global._
import com.orch.daemon._

/**
  * A bunch of stuff that allows to manage VMs with Docker hypervisor
  */
object Docker {
    import com.orch.common._
    import com.orch.daemon.docker._
    import com.orch.daemon.DaemonProto._
    import com.orch.utils.{Path, Exe}

    var mac_of_vms : Map [Long, String] = Map ()
    var ips_of_vms : Map [Long, String] = Map ()
    var booting_vms : Map [Long, Thread] = Map ()
    val boot_conflict : Integer = new Integer (0)


    // var NFS_ADDR = "42.42.1.102"

    // /**
    //   * Start nfs server to give access to it for the VMs
    //   */
    // def startNfsDir (subNet : String = "42.42.1.0/24", nfsAddr : String = "42.42.1.102") : Unit = {
    //     val export_line = "\"" +  Global.nfs_path + "\" " + subNet + "(rw,no_subtree_check,all_squash,anonuid=0,anongid=0,fsid=180077614)\n"
    //     val file = new java.io.File (Path ("/etc/exports").file);
    //     if (file.exists)
    //         file.delete;

    //     Path.writeFile (Path ("/etc/exports"), export_line, true)
    //     val exe = new Exe (Seq ("exportfs", "-ra"), Path ("."))
    //     exe.synchro ()

    //     val exe_2 = new Exe (Seq ("service", "nfs-kernel-server", "start"), Path ("."))

    //     NFS_ADDR = nfsAddr
    // }


    /**
      *  Create a new Container and boot it
      *  Params : 
      *  - id, the id of the VM
      *  - capas, the capacities of the VM (assume only Cpu and Memory)
      */
    def installAndStartDocker (user : String, id : Long, os : String, capas : Map [String, Long]) : Unit = {
        // command docker create --name d_id os --cpus=x --memory=y
        val path_user_vm = Path.buildStr (Seq ("/home/phil/", user)).file;
        Path.mkdirs (Path.buildStr (Seq (Global.nfs_path, user)));

        val path = Path.buildStr (Seq (Global.user_home, user)).file

        val create = new Exe (Seq ("docker", "create", s"--cpus=${capas("cpus")}", s"--memory=${capas ("memory")}m", "-v", s"${path}:${path_user_vm}", "--name", s"d_${id}", os), Path("."));
        val (status_, out_, err_) = create.synchro ();
        println (status_ + " " + out_ + " " + err_);

        val start = new Exe (Seq ("docker", "start", s"d_${id}"), Path ("."));
        val (status, out, err) = start.synchro ();
        println (status + " " + out + " " + err);

        // val cmd = "mkdir -p " + path_user_vm.file + s"; sudo mount ${NFS_ADDR}:" + path + " " + path_user_vm.file;

        // val mount = new Exe (Seq ("docker", "exec", "d_${it}", cmd), Path ("."));
        // mount.synchro ();
        this.ips_of_vms.synchronized {
            this.ips_of_vms = this.ips_of_vms + (id-> "")
        }
    }

    /**
      *  Launch a new Docker
      *  This will call the master when the VM is ready
      */
    def launchContainer (mid : String, id : Long, os : String, capas : Map [String, Long], user : String, script : String, ref : ActorSelection) : Unit = {
        val thread = new Thread () {
            override def run () : Unit = {
                try {                    
                    installAndStartDocker (user, id, os, capas)
                    ref ! DaemonProto.VmReady (mid, id, "")
                } catch {
                    case _ : InterruptedException => {}
                }
            }
        }

        this.booting_vms.synchronized {
            booting_vms = booting_vms + (id -> thread)            
        }

        thread.start ()
    }

    /**
      *  Remove a vm from the list of currently booting VM
      */
    def removeBooting (id : Long) : Unit = {
        this.booting_vms.synchronized {
            this.booting_vms = this.booting_vms.filterKeys (_ != id)
        }
    }


    def getListOfFiles(dir: String):List[java.io.File] = {
        val d = new java.io.File(dir)
        if (d.exists && d.isDirectory) {
            d.listFiles.toList
        } else {
            List[java.io.File]()
        }
    }


    /**
      *  Run a task in a VM
      *  Params : 
      *  - mid, the id of the node (used only for the protocol of communication with the master)
      *  - id, the id of the task
      *  - user, the user to whom the task belong
      *  - v_id, the id of the VM in which we want the task to be run
      *  - params, the parameters to pass to the task
      *  - ref, the actor selection, reference of the Master module
      * This will inform the master module, in case of success or failure
      */
    def runTask (mid : String, wid : String, id : Long, user : String, v_id : Long, params : String, ref : ActorSelection) : Unit = {
        val e_path = Path.buildStr (Seq (Global.user_home, user, wid, ""+ id))
        val path_in_vm = Path.buildStr (Seq ("/home/phil", user, wid, "" + id))

        val innerCommand = "cd " + path_in_vm.file + " ; echo $(pwd) ; ./launch.sh " + params;

        val thread = new Thread () {
            override def run () : Unit = {
                val untar = new Exe (Seq ("tar", "xf", "exec.tar"), e_path)
                untar.synchro ();

                val chmod = new Exe (Seq ("chmod", "777", "."), e_path);
                println (chmod.synchro ());
                println (innerCommand);

                val list = getListOfFiles (e_path.file);

                while (true) {
                    try {
                        val exe = new Exe (Seq ("docker", "exec", s"d_${v_id}", s"bash",  "-c", innerCommand), Path ("."));
                        val (status, out, err) =  exe.synchro ();
                        println (id + " " + status + " " + out + " " + err)
                        for (f <- list) { // remove all input and exec
                            deleteRecursively (f);
                        }

                        if (status != 0) {
                            ref ! DaemonProto.TaskFailure (mid, wid, id, out)
                        } else ref ! DaemonProto.TaskEnd (mid, wid, id)
                        return
                    } catch {
                        case e: Throwable => {
                            println (e)
                            Thread.sleep (200)
                        }
                    }
                }
            }
        }



        thread.start ()
    }

    /**
      *  Kill a VM, booting, running of already killed, that's not important
      *  Params : 
      *  - mid, the id of the node (just for the master module)
      *  - id, the id of the VM to be killed
      *  - ref, the actor selection, reference of the master module
      */
    def killContainer (mid : String, id : Long, ref : ActorSelection) {
        val thread = new Thread () {
            override def run () {
                booting_vms.synchronized {
                    if (booting_vms.contains (id)) {
                        println (s"INTERRUPT : ${id}")
                        booting_vms (id).interrupt ()
                        removeBooting (id)
                    }
                }

                val destroy = new Exe (Seq ("docker", "kill", s"d_$id"), Path ("."))
                val undefine = new Exe (Seq ("docker", "rm", s"d_$id"), Path ("."))

                destroy.synchro ()
                undefine.synchro ()

                ref ! DaemonProto.VmOff (mid, id, "")
                ips_of_vms.synchronized {
                    ips_of_vms = ips_of_vms - id;
                }
            }
        }

        thread.run ()
    }

    def killAllContainer () : Unit = {
        for (v <- ips_of_vms) {
            val destroy = new Exe (Seq ("docker", "kill", s"d_${v._1}"), Path ("."))
            val undefine = new Exe (Seq ("docker", "rm", s"d_${v._2}"), Path ("."))
            destroy.synchro ()
            undefine.synchro ()
        }
    }

    def deleteRecursively(file: java.io.File): Unit = {
        if (file.isDirectory)
            file.listFiles.foreach(deleteRecursively)

        if (file.exists)
            file.delete
    }


}
