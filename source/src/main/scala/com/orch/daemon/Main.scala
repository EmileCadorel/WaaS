package com.orch.daemon

import java.io.{File}
import scala.Array
import akka.actor.{Props, Actor, ActorSystem, ActorRef, ActorLogging , ActorSelection }
import com.typesafe.config.ConfigFactory

import akka.util.ByteString
import java.net.InetSocketAddress

object Main {
    import scopt.OParser
    import com.orch.daemon.Options._
    import com.orch.daemon.kvm.KVMDaemon
    import com.orch.daemon.vmware.VMWDaemon
    import com.orch.daemon.docker.DockerDaemon
    import com.orch.daemon.vmware._

    def propsKVM (addr : String, port : Int, maddr : String, mport : Int, configFile : String) : Props = Props (new KVMDaemon (addr, port, maddr, mport, configFile))
    def propsDocker (addr : String, port : Int, maddr : String, mport : Int, configFile : String) : Props = Props (new DockerDaemon (addr, port, maddr, mport, configFile))

    def configFile (addr: String, port: Int): String = {
        s"""akka {
      loglevel = "INFO"
      log-dead-letters-during-shutdown=off
      log-dead-letters=off
      actor {
        warn-about-java-serializer-usage=off
        provider = "akka.remote.RemoteActorRefProvider"
      }
      remote {
        log-remote-lifecycle-events=off
        enabled-transports = ["akka.remote.netty.tcp"]
        netty.tcp {
          hostname = $addr
          port = $port
        }
        log-sent-messages = on
        log-received-messages = on
      }
    }"""
    }

    def parseOptions (args : Array[String]) : Options.Config = {
        OParser.parse(Options.parser1, args, Options.Config()).getOrElse (Options.Config ())
    }

    def main (args : Array[String]) {
        val config = parseOptions (args)

        val file_config = ConfigFactory.parseString (configFile (config.addr, config.port))
        val system = ActorSystem ("RemoteSystem", file_config)
        val master = if (config.vm == "Docker")
            system.actorOf (Main.propsDocker (config.addr, config.port, config.maddr, config.mport, config.file), name="daemon")
        else
            system.actorOf (Main.propsKVM (config.addr, config.port, config.maddr, config.mport, config.file), name="daemon")
    }
}
