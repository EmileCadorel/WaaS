package com.orch.daemon

import akka.remote.DisassociatedEvent
import java.io.{File}
import scala.Array
import akka.actor.{Props, Actor, ActorSystem, ActorRef, ActorLogging , ActorSelection, PoisonPill  }
import com.typesafe.config.ConfigFactory

import akka.util.ByteString
import java.net.InetSocketAddress
import java.io._
import java.util.{Map => JMap, List => JArray}
import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConverters._
import com.orch.common._

/**
  * The daemon class is in charge of one or multiple nodes
  * It possess an ip addr DAddr and a port DPort
  * It can access to the master Module via the MAddr:MPort, ipv4 addr
  * The configFile define the nodes that are managed by this Daemon
  * This is a ancestor class, that will do nothing when message are sent to it
  * It is only intended to register to the master Module, and send to it information about the node it is in charge of
  */
abstract class Daemon (DAddr : String, DPort : Int, MAddr : String, MPort : Int, configFile : String) extends Actor with ActorLogging {

    import com.orch.daemon.DaemonProto._

    var masterModule : ActorSelection = null
    var node : Node = null
    var boot_times : Map [String, Long] = Map ()

    /**
      * Read the node that is managed by this daemon
      */
    final def parseNodeFile () : Node = {
        val yaml : Yaml = new Yaml
        val root = yaml.load (new FileInputStream (new java.io.File (configFile)))
        val node = root.asInstanceOf[JMap[String, Object]].asScala
        val name = node ("name").asInstanceOf [String]
        val speed = node ("speed").asInstanceOf [Int]
        var capas : Map [String, Long] = Map ()
        for (k <- node ("capacities").asInstanceOf [JMap[String, Int]].asScala)
            capas += (k._1 -> (k._2).toLong)

        for (b <- node ("boot_times").asInstanceOf [JMap[String, Int]].asScala)
            boot_times += (b._1 -> (b._2).toLong)

        Node (name, capas, speed, "", DAddr, DPort)
    }

    /**
      * Start the daemon, by reading the configuration and send information to the master Module
      */
    override def preStart () : Unit = {
        context.system.eventStream.subscribe(self, classOf[DisassociatedEvent]);

        log.info ("Connecting to Master Module ... ");
        masterModule = context.actorSelection (s"akka.tcp://RemoteSystem@$MAddr:$MPort/user/master");
        log.info ("Load configuration ... ");
        node = parseNodeFile ()
        log.info ("Inform master ...");
        masterModule ! DaemonProto.InformMaster (DAddr, DPort, node, boot_times)
        onStart ()
        log.info ("Daemon started and ready to recv message !")
        context.become (run)
    }

    def onStart () = {}

    def receive : Receive = {
        case _ => {}
    }

    def run : Receive

    override def postStop () : Unit = {
        log.info ("Daemon killed!")
    }
}
