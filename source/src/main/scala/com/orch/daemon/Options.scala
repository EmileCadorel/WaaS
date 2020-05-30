package com.orch.daemon

import scopt.OParser

object Options {

    case class Config(
        addr : String = "127.0.0.1",
        port : Int = 5200,
        maddr : String = "127.0.0.1",
        mport : Int = 5100,
        file : String = "config.yaml",
        vm : String = "KVM"
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

            opt[String]('m', "maddr")
                .action((x, c) => c.copy(maddr = x))
                .text("address of the master module"),

            opt[Int]('p', "port")
                .action ((x, c) => c.copy (port = x))
                .text ("local port"),

            opt[Int]('x', "mport")
                .action ((x, c) => c.copy (mport = x))
                .text ("master port"),

            opt[String]('c', "config")
                .action ((x, c) => c.copy (file = x))
                .text ("configuration file"),
            opt[String]('t', "type")
                .action ((x, c) => c.copy (vm = x))
                .text ("virtualisation system")

        )
    }

}
