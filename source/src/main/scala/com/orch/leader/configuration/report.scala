package com.orch.leader.configuration

class Report (val vms : Map [String, Map [Long, CVM]], val tasks : Map [String, Map [String, CTask]], val files : Map [String, Map [String, Array [CFile]]]) {

    def prettyPrint () : Unit = {
        println ("VMS : {")
        for (v <- vms)
            println ("\t" + v);
        println ("}")

        println ("Tasks : {")
        for (t <- tasks)
            println ("\t" + t);
        println ("}")

        println ("Files : {")
        for (f <- files)
            println ("\t" + f);
        println ("}");
    }

}
