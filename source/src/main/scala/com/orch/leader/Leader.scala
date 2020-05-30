package com.orch.leader

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
class Leader (name : String, LAddr : String, LPort : Int, LBW : Int, SCHED : String) extends Actor with ActorLogging {

    import com.orch.utils.{Path}
    import com.orch.master.MasterProto._
    import com.orch.leader.scheduling._
    import com.orch.leader.workflow._    
    import com.orch.leader.scheduling.heft._
    import com.orch.leader.scheduling.min_min._
    import com.orch.leader.scheduling.max_min._
    import com.orch.leader.scheduling.heft_deadline._
    import com.orch.leader.configuration._
    import com.orch.leader.election.DumbElector
    import com.orch.leader.FileIO
    import com.orch.master.MasterProto

    /**
      * The scheduler is used to affect task to vm to nodes
      */
    var scheduler : Scheduler = chooseScheduler () ;


    /**
      * The full graph of communication
      */
    var peerModules : Map [String, ActorSelection] = Map ()
    var peerModulesInfos : Map [String, (String, Int)] = Map ()

    /**
      * To have uniq ids for each new workflow, only the leader has the right to affect an id to a workflow
      */
    var __lastFlow__ = 0;

    var runningFlows : Map [String, Workflow] = Map ()

    def chooseScheduler () : Scheduler = {
        if (SCHED.toUpperCase () == "HEFT")
            new HeftScheduler (new DumbElector ())
        else if (SCHED.toUpperCase () == "HEFT_DEADLINE")
            new HeftDeadlineScheduler (new DumbElector ())
        else if (SCHED.toUpperCase () == "MIN_MIN")
            new MinMinScheduler (new DumbElector ())
        else if (SCHED.toUpperCase () == "MAX_MIN")
            new MaxMinScheduler (new DumbElector ())
        else
            new HeftScheduler (new DumbElector ())
    }

    override def preStart () : Unit = {
        log.info ("ICI?")
        Global.flow_home = "/tmp/master" + s"@${LAddr}_5000" + "/"

    }

    def receive : Receive = {

        /**
          * ===============================================
          *               Master Messages
          * ===============================================
          */

        case AskForWelcome (name, ip, port, bw) =>
            log.info ("Leader welcoming new master ...")
            val entry = context.actorSelection (s"akka.tcp://RemoteSystem@${ip}:${port}/user/master")
            this.scheduler.informBwCluster (name, bw)
            peerModules = peerModules + (name-> entry)
            peerModulesInfos = peerModulesInfos + (name-> (ip, port))

            // We insert the new module between this and this.right
            entry ! Welcome (peerModulesInfos, LAddr, LPort, this.scheduler.getZeroInstant ())


        /**
          * ===============================================
          *               Cluster Messages
          * ===============================================
          */

        case NewNode (cluster, node, boots) =>
            log.info (s"Add new ${node.n_id} node in scheduler ...")
            this.scheduler.addNewNode (cluster, node)
            this.scheduler.informBootTimes (node.n_id, boots)

        case BwNetwork (clusterA, clusterB, bw) =>
            log.info (s"Information about bandwidth between ${clusterA} and ${clusterB} is ${bw} KBytes/sec ...")
            this.scheduler.informBwNetwork (clusterA, clusterB, bw)

        /**
          * ===============================================
          *               Submission Messages
          * ===============================================
          */            


        case NewWorflow (addr, port, user, path, attributes) =>
            __lastFlow__ += 1
            val flowId = __lastFlow__ + s"-master@${LAddr}_5000";

            log.info (s"Start download of flow : ${flowId}");
            // We start be downloading the workflow files
            // If the workflow has been submitted here, maybe the client does not have access to the network of the leader

            FileIO.recvZip (self, addr, port, user, path, flowId, attributes)

        case WorkflowUploaded (id, user, addr, port, attributes) =>
            log.info (s"Workflow $id has been successfully downloaded to ${Global.flow_home}")
            val client = context.actorSelection (s"akka.tcp://RemoteSystem@$addr:${port - 1}/user/client")
            // The workflow has been upload, we inform the client
            client ! MasterProto.WorkflowUploaded (id, user, addr, port, attributes)
            workflow.Loader.unzipFlow (id, user)
            
            workflow.Loader.createFlow (self, "master" + s"@${LAddr}_5000", id, user, Path.buildStr (Seq (Global.flow_home, "" + id)).file, attributes)

        case WorkflowLoaded (id, flow) =>
            log.info (s"Workflow successfully loaded by the leader ${id}")
            val config = this.scheduler.onScheduling (flow)
            this.runningFlows = this.runningFlows + (flow.id -> flow)

            for (cluster <- config.vms) {
                // If the configuration is empty, no need to bother the cluster
                if (!config.vms (cluster._1).isEmpty && !config.tasks (cluster._1).isEmpty && !config.files (cluster._1).isEmpty) {
                    log.info ("Sending configuration to cluster : " + cluster._1);
                    val node = peerModules (cluster._1)
                    node ! UpdateConfiguration (config.vms (cluster._1), config.tasks (cluster._1), config.files (cluster._1))
                }
            }

        case OutputReceived (wid, fid) =>
            this.runningFlows (wid).removeOutputFile (fid)
            if (this.runningFlows (wid).isFinished) {
                sendMail (this.runningFlows (wid))
            }

    }


    def sendMail (flow : Workflow) : Unit = {
        log.info (s"Workflow ${flow.id} is finished")
    }

}
