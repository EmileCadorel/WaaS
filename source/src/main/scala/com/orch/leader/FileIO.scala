package com.orch.leader

import java.net.ServerSocket
import java.io.IOException
import akka.actor.{ Actor, ActorRef, Props , ActorContext, ActorSelection }
import scala.collection.mutable.Queue

import akka.io.{ IO, Tcp }
import akka.actor.{ Actor, ActorRef, Props }

/**
  * FileIO is a simple file transfert module
  * It use curl to acquire files from external nodes
  * And provide a http server, that give access to the files
  */
object FileIO {

    import com.orch.utils.{Path, Exe}
    import com.orch.master.MasterProto
    
    val nb_slot = 10
    var nb_sending = 0
    var nb_recv = 0
    var current_recv : Map [Int, (Exe, Long)] = Map ()
    var current_send : Map [Int, (Exe, Long)] = Map ()

    var send_list : Queue [Exe] = Queue ()
    var used_port : List [Int] = List ()

    /**
      * Enqueue a new sending file in the waiting list
      * If there is an available slot, it launch the receiving
      */
    def launchRecv () : Unit = {
        this.send_list.synchronized {
            if (nb_sending < nb_slot && send_list.length != 0) {
                val fst = send_list.dequeue
                nb_sending += 1
                fst.start ()
            } else {
            }
        }
    }

    /**
      * Trigger that is triggered when a file has been received
      * It call launchRecv
      */
    def onRecvEnd () : Unit = {
        this.send_list.synchronized {
            nb_sending -= 1
            launchRecv ()
        }
    }

    /**
      * Start the Http server that give access the the files
      */
    def startServer (path : String = "/tmp/", port : Int) {
        val mkdir = new Exe (Seq ("mkdir", "-p", path), Path ("."));
        mkdir.synchro ();

        new Exe (Seq ("ruby", "-run", "-ehttpd", path, s"-p${port}"), Path ("."),
            (_,_,_) => {},
            (msg,_) => {
                false
            }
        ).start ()
        Thread.sleep (1000)
    }

    def recvZip (remote : ActorRef, addr : String, port : Int, user : String, path : String, id : String, attributes : Map [String, Long]) : Unit = {
        val remote_file = new java.io.File (path)
        val flow_path = Path.buildStr (Seq (Global.flow_home, "" + id)).file;
        val file = Path.buildStr (Seq (Global.flow_home, "" + id, id + ".tgz")).file;
        val mkdir = new Exe (Seq ("mkdir", "-p", flow_path), Path ("."),
            (_,_,_) =>  {
                new Exe (Seq ("curl", addr + ":" + port + "/" + path, "-o", file), Path ("."),
                    (_, _, _) => {
                        remote ! MasterProto.WorkflowUploaded (id, user, addr, port, attributes)
                    },
                    (_, _) => {
                        true
                    }
                ).start ()
            },
            (_,_) => {
                true
            }
        );
        mkdir.start ();
    }

    /**
      * Filename is the filename of the file, and fid is its identifier
      */
    def recvInput (remote : ActorRef, addr : String, port : Int, wid : String, fid : String, filename : String) : Unit = {
        val remote_file = Path.buildStr (Seq ("/", wid, "input", filename)).file
        val flow_path = Path.buildStr (Seq (Global.flow_home, wid, "input")).file
        val file = Path.buildStr (Seq (flow_path, filename)).file
        val mkdir = new Exe (Seq ("mkdir", "-p", flow_path), Path ("."),
            (_,_,_) => {
                new Exe (Seq ("curl", (addr + ":" + port) + remote_file, "-o", file), Path ("."),
                    (_,_,_)=> {
                        remote ! MasterProto.InputReceived (wid, fid)
                    },
                    (code,err)=> {
                        println (code + " " + err);
                        false
                    }
                ).start ()
                },
            (_,_)=> {
                true
            }
        );
        mkdir.start ()
    }

    /**
      * Filename is the filename of the file, and fid is its identifier
      */
    def recvTrans (remote : ActorRef, addr : String, port : Int, wid : String, fid : String, filename : String) : Unit = {
        val remote_file = Path.buildStr (Seq ("/", wid, "trans", filename)).file
        val flow_path = Path.buildStr (Seq (Global.flow_home, wid, "trans")).file
        val file = Path.buildStr (Seq (flow_path, filename)).file
        val mkdir = new Exe (Seq ("mkdir", "-p", flow_path), Path ("."),
            (_,_,_) => {
                new Exe (Seq ("curl", (addr + ":" + port) + remote_file, "-o", file), Path ("."),
                    (_,_,_)=> {
                        remote ! MasterProto.TransfertReceived (wid, fid)
                    },
                    (code,err)=> {
                        println (code + " " + err);
                        false
                    }
                ).start ()
                },
            (_,_)=> {
                true
            }
        );
        mkdir.start ()
    }

    /**
      * Filename is the filename of the file, and fid is its identifier
      */
    def recvOutput (remote : ActorRef, addr : String, port : Int, wid : String, fid : String, filename : String) : Unit = {
        val remote_file = Path.buildStr (Seq ("/", wid, "output", filename)).file
        val flow_path = Path.buildStr (Seq (Global.flow_home, wid, "output")).file
        val file = Path.buildStr (Seq (flow_path, filename)).file
        val mkdir = new Exe (Seq ("mkdir", "-p", flow_path), Path ("."),
            (_,_,_) => {
                new Exe (Seq ("curl", (addr + ":" + port) + remote_file, "-o", file), Path ("."),
                    (_,_,_)=> {
                        remote ! MasterProto.OutputReceived (wid, fid)
                    },
                    (code,err)=> {
                        println (code + " " + err);
                        false
                    }
                ).start ()
                },
            (_,_)=> {
                true
            }
        );
        mkdir.start ()
    }

    def transfertForRemote (remote : ActorRef, addr : String, port : Int, clusters : Array [String], wid : String, fid : String, path : String, name : String) : Unit = {
        val flow_path = Path.buildStr (Seq (Global.flow_home, wid, "trans")).file
        val file = Path.buildStr (Seq (flow_path, name)).file
        val mkdir = new Exe (Seq ("mkdir", "-p", flow_path), Path ("."),
            (_,_,_) => {
                new Exe (Seq ("curl", (addr + ":" + port) + path, "-o", file), Path ("."),
                    (_,_,_)=> {
                        remote ! MasterProto.TransfertForRemote (clusters, wid, fid, name)
                    },
                    (code,err)=> {
                        println (code + " " + err);
                        false
                    }
                ).start ()
            },
            (_,_)=> {
                true
            }
        );
        mkdir.start ()
    }

    def downloadForRemote (remote : ActorRef, addr : String, port : Int, clusters : Array [String], wid : String, fid : String, path : String, name : String) : Unit = {
        val flow_path = Path.buildStr (Seq (Global.flow_home, wid, "output")).file
        val file = Path.buildStr (Seq (flow_path, name)).file
        val mkdir = new Exe (Seq ("mkdir", "-p", flow_path), Path ("."),
            (_,_,_) => {
                new Exe (Seq ("curl", (addr + ":" + port) + path, "-o", file), Path ("."),
                    (_,_,_)=> {
                        remote ! MasterProto.DownloadForRemote (clusters, wid, fid, name)
                    },
                    (code,err)=> {
                        println (code + " " + err);
                        false
                    }
                ).start ()
            },
            (_,_)=> {
                true
            }
        );
        mkdir.start ()
    }

    def recvExec (remote : ActorRef, addr : String, port : Int, wid : String, ids : Array [Long], app : String) : Unit = {
        val remote_flow_path = Path.buildStr (Seq ("/", wid, app)).file // The path is in the app name
        val flow_path = Path.buildStr (Seq (Global.flow_home, wid)).file
        val file = Path.buildStr (Seq (flow_path, app)).file
        val mkdir = new Exe (Seq ("mkdir", "-p", flow_path), Path ("."),
            (_,_,_) => {
                new Exe (Seq ("curl", (addr + ":" + port) + remote_flow_path, "-o", file), Path ("."),
                    (_,_,_)=> {
                        remote ! MasterProto.ExecutableReceived (wid, remote_flow_path, ids)
                    },
                    (code,err)=> {
                        println (code + " " + err);
                        true
                    }
                ).start ()
                },
            (_,_)=> {
                true
            }
        );
        mkdir.start ()
    }

    def recvFlowConfig (addr : String, port : Int,  id : String) : Unit = {
        val flow_path = Path.buildStr (Seq (Global.flow_home, "" + id)).file;

        val remoteFlowYaml = Path.buildStr (Seq ("" + id, "flow.yaml")).file;
        val remoteCustomYaml = Path.buildStr (Seq ("" + id, "custom.yaml")).file;

        val flowYaml = Path.buildStr (Seq (Global.flow_home, "" + id, "flow.yaml")).file;
        val customImg = Path.buildStr (Seq (Global.flow_home, "" + id, "custom.yaml")).file;

        val mkdir = new Exe (Seq ("mkdir", "-p", flow_path), Path ("."));
        mkdir.synchro ();

        val recvFlow = new Exe (Seq ("curl", addr + ":" + port + "/" + remoteFlowYaml, "-o", flowYaml), Path ("."));
        val recvCustom = new Exe (Seq ("curl", addr + ":" + port + "/" + remoteCustomYaml, "-o", customImg), Path ("."));

        recvFlow.synchro ();
        recvCustom.synchro ();
    }

}
