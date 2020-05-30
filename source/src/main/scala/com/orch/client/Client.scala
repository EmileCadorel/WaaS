package com.orch.client

import akka.remote.DisassociatedEvent
import java.io.{File}
import scala.Array
import akka.actor.{Props, Actor, ActorSystem, ActorRef, ActorLogging , ActorSelection, PoisonPill }
import com.typesafe.config.ConfigFactory

import akka.util.ByteString
import java.net.InetSocketAddress
import java.io._
import java.util.{Map => JMap, List => JArray}
import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConverters._
import com.orch.common._
import com.orch.master.MasterProto


/**
  The client is an actor launched by an user that want to submit a new workflow for execution
  */
class Client (LAddr : String, LPort : Int, MAddr : String, MPort : Int, user : String, path : String, attributes : Map [String, Long]) extends Actor with ActorLogging {

    var master : ActorSelection = null

    override def preStart () : Unit = {
        log.info (s"New client for file : $path");

        log.info ("Connecting to master ...")
        master = context.actorSelection (s"akka.tcp://RemoteSystem@$MAddr:$MPort/user/leader")

        log.info ("Start http server...")
        FileIO.startServer (LPort + 1, (new java.io.File (path)).getParent ())

        log.info ("Upload to master...")
        master ! MasterProto.NewWorflow (LAddr, LPort + 1, user, (new java.io.File (path)).getName (), attributes)
    }

    def receive : Receive = {
        case MasterProto.WorkflowUploaded (id,_,_,_,_) =>
            log.info (s"Workflow successfully $id uploaded")
            log.info ("Terminate client");
            self ! PoisonPill // Close the client everything is done for it
            context.system.terminate ();
            System.exit (0);
    }
}

