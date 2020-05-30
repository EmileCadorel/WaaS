package com.orch.leader

import scopt.OParser

object Options {

    case class Config(
        addr : String = "127.0.0.1",
        port : Int = 5100,
        eaddr : String = "",
        eport : Int = 0,
        name : String = "leader",
        bw : Int = 10000000, // Kbytes
        sched : String = "HEFT"
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

            opt[String]('n', "name")
                .action ((x, c) => c.copy (name = x))
                .text ("the name of the module"),

            opt[Int]('b', "bw")
                .action ((x, c) => c.copy (bw = x))
                .text ("the bandwidth in the cluster"),

            opt[String]('s', "sched")
                .action ((x, c) => c.copy (sched = x))
                .text ("the scheduler to use")
        )
    }

}
