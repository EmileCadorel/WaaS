package com.orch.client

import java.net.ServerSocket
import java.io.IOException
import akka.actor.{ Actor, ActorRef, Props , ActorContext, ActorSelection }
import scala.collection.mutable.Queue

import akka.io.{ IO, Tcp }
import akka.actor.{ Actor, ActorRef, Props }
import com.orch.utils._

/**
  * FileIO is a simple file transfert module
  * It use curl to acquire files from external nodes
  * And provide a http server, that give access to the files
  */
object FileIO {

    /**
      * Start the Http server that give access the the files
      */
    def startServer (port : Int, path : String) {
        new Exe (Seq ("ruby", "-run", "-ehttpd", path, s"-p$port"), Path ("."),
            (_,_,_) => {},
            (msg,_) => {
                false
            }
        ).start ()
        Thread.sleep (1000)
    }

}
