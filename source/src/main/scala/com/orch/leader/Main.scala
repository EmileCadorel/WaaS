package com.orch.leader

import java.io.{File}
import scala.Array
import akka.actor.{Props, Actor, ActorSystem, ActorRef, ActorLogging , ActorSelection }
import com.typesafe.config.ConfigFactory

import akka.util.ByteString
import java.net.InetSocketAddress

object Main {
    import scopt.OParser
    import com.orch.master.Master
    import com.orch.master.Options._

    def props (name : String, addr : String, port : Int, eaddr : String, eport : Int, bw : Int, sched : String) : Props = Props (new Leader (name + s"@${addr}_${port}", addr, port, bw, sched))


    def configFile (addr: String, port: Int):String = {
        s"""akka {
      loglevel = "INFO"
      log-dead-letters-during-shutdown=off
      log-dead-letters=off
      actor {
        warn-about-java-serializer-usage=off
        provider = "akka.remote.RemoteActorRefProvider"
      }
      remote {
        maximum-payload-bytes = 30000000 bytes
        log-remote-lifecycle-events=off
        enabled-transports = ["akka.remote.netty.tcp"]
        netty.tcp {
          hostname = $addr
          port = $port
          message-frame-size =  30000000b
          send-buffer-size =  30000000b
          receive-buffer-size =  30000000b
          maximum-frame-size = 30000000b
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
        val masterActor = system.actorOf (Main.props (config.name, config.addr, config.port, config.eaddr, config.eport, config.bw, config.sched), name="leader")
    }
}
