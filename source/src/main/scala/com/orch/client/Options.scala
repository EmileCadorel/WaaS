package com.orch.client

import scopt.OParser

object Options {

    case class Config(
        addr : String = "127.0.0.1",
        port : Int = 5100,
        eaddr : String = "",
        eport : Int = 0,
        user : String = "alice",
        path : String = "",
        deadline : Long = 0
    )


    val builder = OParser.builder[Config]

    val parser1 = {
        import builder._
        OParser.sequence(
            programName("shell"),
            head("scopt", "4.x"),
            // option -f, --foo
            opt[String]('a', "addr")
                .action((x, c) => c.copy(addr = x))
                .text("address of the daemon module"),

            opt[String]('e', "eaddr")
                .action((x, c) => c.copy(eaddr = x))
                .text("address of the entry module"),

            opt[Int]('p', "port")
                .action ((x, c) => c.copy (port = x))
                .text ("local port"),

            opt[Int]('x', "eport")
                .action ((x, c) => c.copy (eport = x))
                .text ("entry port"),

            opt[String]('u', "user")
                .action ((x, c) => c.copy (user = x))
                .text ("the user name"),

            opt[String]('p', "path")
                .action ((x, c) => c.copy (path = x))
                .text ("the path"),
            
            opt[Long] ('d', "deadline")
                .action ((x, c) => c.copy (deadline = x))
                .text ("the deadline")
        )
    }

}
