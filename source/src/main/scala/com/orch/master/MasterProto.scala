package com.orch.master

import com.orch.leader.workflow._
import com.orch.leader.configuration._
import com.orch.common._

object MasterProto {

    /**
      * ===============================================
      *               Scheduling Messages
      * ===============================================
      */

    case class NewNode (cluster : String, node : Node, boots : Map [String, Long])

    case class BwNetwork (clusterA : String, clusterB : String, bw : Int)

    /**
      * ===============================================
      *               Peer Messages
      * ===============================================
      */

    /**
      * When a new master is created and is introduce by some other master
      * It ask for a welcome, the other Master module will send to it the list of neighbour
      */
    case class AskForWelcome (name : String, ip : String, port : Int, bw : Int)


    /**
      * Message of welcome send by a master module the another one
      * This message is the list of neighbour in the topology
      */
    case class Welcome (topology : Map [String, (String, Int)], addr : String, port : Int, zero : Long)


    /**
      * When a new master is created it connect itself to the other Master Modules 
      * The hand shake is just an information to the other modules, 
      * that will store the new actor in the list of neighbour
      */
    case class PeerHandShake (name : String, ip : String, port : Int)

    /**
      * ===============================================
      *               Leader Election
      * ===============================================
      */

    /**
      * When there is no master in the topology we need to elect a new one
      * The first thing to do is to init the ring, and choose the candidates
      * This election is based on the election leader on a complete graph 
      * The election is launched when the leader is disconnected 
      * But we need to make the ring first ?
      * Cf: related/cites:leader-election
      */
    case class createRing (name : String, taken : Set [String])

    /**
      * Change the left element of the ring
      */
    case class ChangeLeft (name : String)

    /**
      * Change the right element of the ring
      */
    case class ChangeRight (name : String)

    /**
      *  ===========================================
      *           Workflows Submission
      *  ===========================================
      */

    /**
      * Message send, when a new workflow is submitted by a user, 
      * The workflow informations are located in a http server, at the path path
      * The master will retreive all the files an create the topology of the workflow
      */
    case class NewWorflow (addr : String, port : Int, user : String, path : String, attributes : Map [String, Long])

    /**
      * Sent by the FileIO system, this inform the server prototype
      * that a new workflow is uploaded and ready to be analysed
      */
    case class WorkflowUploaded (id : String, user : String, addr : String, port : Int, attributes : Map [String, Long])

    /**
      * Message treated by the leader when a new workflow is submitted somewhere
      */
    case class ScheduleFlow (id : String, user : String, remote : String, attributes : Map [String, Long])
    
    /**
      * 
      */
    case class WorkflowLoaded (id : String, flow : Workflow)

    /**
      *  ===========================================
      *           Configuration Update 
      *  ===========================================
      */

    /**
      * Message sent by the leader, it define a update of the configuration
      */
    case class UpdateConfiguration (vms : Map [Long, CVM], tasks : Map [String, CTask], files : Map [String, Array [CFile]])

    /**
      * Wake up message to inform the actor that he needs to run the configuration
      */
    case class PlayConfiguration ()

    /**
      *  ===========================================
      *           Configuration Running
      *  ===========================================
      */

    /**
      * Triggered when the executable has been uploaded to the cluster
      */
    case class ExecutableReceived (wid : String, app : String, ids : Array [Long])

    /**
      * Triggered when an input file from a remote cluster has been received
      */
    case class InputReceived (wid : String, name : String)

    /**
      * When a file has been downloaded from a remote cluster, that has generated it
      */
    case class TransfertReceived (wid : String, name : String)

    /**
      * When an output file is received
      */
    case class OutputReceived (wid : String, name : String)

    /**
      * Triggered when a remote cluster has created a file
      */
    case class TransfertFromRemote (addr : String, port : Int, wid : String, fid : String, path : String)

    /**
      * Triggered when a task has created a file that has to be sent to a remote cluster
      */
    case class TransfertForRemote (clusters : Array [String], wid : String, fid : String, path : String)

    /**
      * Triggered when a remote cluster has created a file
      */
    case class DownloadFromRemote (addr : String, port : Int, wid : String, fid : String, path : String)

    /**
      * Triggered when a task has created a file that has to be sent to a remote cluster
      */
    case class DownloadForRemote (clusters : Array [String], wid : String, fid : String, path : String)

}
