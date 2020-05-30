package com.orch.daemon

import com.orch.common._

/**
  * The daemonProto, is used to communicate between the Master Module, and the Daemon modules
  * Each master module on each cluster is able to communicate with the daemon of the same cluster 
  */
object DaemonProto {

    /**
      * ======================================================================================          
      *                                     MASTER
      * ======================================================================================
      */
    case class InformMaster (addr : String, port : Int, nodes : Node, boots : Map [String, Long])

    /**
      * ======================================================================================          
      *                                        VMS
      * ======================================================================================
      */
    case class LaunchVM  (mid : String, id : Long, os : String, capas : Map [String, Long], user : String, script : String)
    case class PauseVM   (mid : String, id : Long)
    case class ResumeVM  (mid : String, id : Long)
    case class KillVM    (mid : String, id : Long)
    case class VmReady   (mid : String, id : Long, log : String)
    case class VmError   (mid : String, id : Long, log : String)
    case class VmResumed (mid : String, id : Long)
    case class VmOff     (mid : String, id : Long, log : String)

    /**
      * ======================================================================================          
      *                                        FILES
      * ======================================================================================
      */
    case class RecvFile       (mid : String, wid : String, id : String, addr : String, port : Int, path : String, user : String, task : Array [Long])

    case class ExeReceived    (mid : String, wid : String, id : Long, user : String, v_id : Long, args : String)
    case class FileSent       (mid : String, wid : String, id : String, tid : Array [Long])
    case class FileFailure    (mid : String, id : String)


    /**
      * ======================================================================================          
      *                                        TASKS
      * ======================================================================================
      */
    case class RunTask     (mid : String, wid : String, id : Long, app : String, user : String, vm : Long, params : String)

    case class TaskEnd     (mid : String, wid : String, id : Long)
    case class TaskFailure (mid : String, wid : String, id : Long, log : String)

    /**
      * ======================================================================================          
      *                                     CLEANING
      * ======================================================================================
      */
    case class RmTaskDir   (mid : String, id : Long, user : String)
    case class RmTaskInput (mid : String, id : Long, user : String, path : String)

    case object Ack

}
