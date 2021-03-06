package com.orch.daemon.docker
import com.orch.daemon.docker._
import com.orch.utils._
import com.orch.daemon._
import akka.remote.DisassociatedEvent
import akka.actor.{PoisonPill}
import java.lang._;
import java.util._;

/**
  *  A kvm daemon is launch on one and only one node
  *  It has the capacity to launch VM with Docker
  */
class DockerDaemon (DAddr : String, DPort : Int, MAddr : String, MPort : Int, configFile : String) extends Daemon (DAddr, DPort, MAddr, MPort, configFile) {

    override def onStart () = {
        log.info ("Start http file server ... ");
        FileIO.startServer ("/tmp/", DPort + 200);
        val local_net = DAddr.slice (0, DAddr.lastIndexOf (".")) + ".0/24"
        // log.info (s"Start nfs dir ${local_net} ... ");
        // Docker.startNfsDir (local_net, DAddr);
    }

    override def run () : Receive = {
        /**
          * ======================================================================================          
          *                                        FILES
          * ======================================================================================
          */

        case DaemonProto.RecvFile (mid, wid, id, addr, port, path, user, tasksId) =>
            log.info (s"Recv input file : ${user}@${addr}:${port}:${path} for tasks : ${Arrays.toString (tasksId)}")
            FileIO.recvFile (masterModule, mid, wid, id, addr, port, path, user, tasksId)

        /**
          * ======================================================================================          
          *                                        VMS
          * ======================================================================================
          */
        case DaemonProto.LaunchVM (mid, id, os, capas, user, script) =>
            log.info (s"Launch vm : $id, with $os of $capas for user $user with $script")
            Docker.launchContainer (mid, id, os, capas, user, script, masterModule)

        case DaemonProto.KillVM (mid, id) =>
            log.info (s"Kill vm : $id")
            Docker.killContainer (mid, id, masterModule)

        /**
          * ======================================================================================          
          *                                        TASKS
          * ======================================================================================
          */
        case DaemonProto.RunTask (mid, wid, id, app, user, vid, params) =>
            log.info (s"Run $id task $app")
            FileIO.recvExe (self, mid, wid, id, MAddr, MPort + 200, app, params, user, vid)

        case DaemonProto.ExeReceived (mid, wid, id, user, v_id, params) =>
            log.info (s"Exe received : ${wid}/${id}")
            Docker.runTask (mid, wid, id, user, v_id, params, masterModule)

        /**
          * ======================================================================================          
          *                                     CLEANING
          * ======================================================================================
          */
        case DaemonProto.RmTaskDir (_, id, user) =>
            val path = Path.buildStr (Seq (Global.user_home, user, "" + id)).file
            log.info (s"Rm $id dir : $path")
            val file = new java.io.File (path)
            deleteRecursively (file)

        case DaemonProto.RmTaskInput (_, id, user, path_) =>
            val path = Path.buildStr (Seq (Global.user_home, user, "" + id, path_)).file
            log.info (s"Rm $path file")
            deleteRecursively (new java.io.File (path))

        /**
          * ======================================================================================          
          *                                     Master Deconnection
          * ======================================================================================
          */

        case DisassociatedEvent (local, remote, inBound) =>
            if (remote.hostPort.indexOf (MAddr + ":" + MPort) != -1) {
                log.error ("We lost our master, system halting !!")
                Docker.killAllContainer ();

                self ! PoisonPill
                context.system.terminate ();
                System.exit (0);
            }

        /**
          * ======================================================================================          
          *                                     UNKNOWN
          * ======================================================================================
          */
        case x => {
            log.error ("Undefined daemon order : " + x);
        }
    }


    def deleteRecursively(file: java.io.File): Unit = {
        if (file.isDirectory)
            file.listFiles.foreach(deleteRecursively)

        if (file.exists)
            file.delete
    }
}

