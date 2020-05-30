package com.orch.client

import java.io.{File}
import scala.Array
import akka.actor.{Props, Actor, ActorSystem, ActorRef, ActorLogging , ActorSelection }
import com.typesafe.config.ConfigFactory

import akka.util.ByteString
import java.net.InetSocketAddress

object Main {
    import scopt.OParser
    import com.orch.client.Client
    import com.orch.client.Options._

    def props (addr : String, port : Int, eaddr : String, eport : Int, user : String, path : String, attributes : Map [String, Long]) : Props = Props (new Client (addr, port, eaddr, eport, user, path, attributes))


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
        var map : Map [String, Long] = Map ()
        map = map + ("deadline"-> config.deadline)

        val file_config = ConfigFactory.parseString (configFile (config.addr, config.port))
        val system = ActorSystem ("RemoteSystem", file_config)
        val clientActor = system.actorOf (Main.props (config.addr, config.port, config.eaddr, config.eport, config.user, config.path, map), name="client")
    }
}
