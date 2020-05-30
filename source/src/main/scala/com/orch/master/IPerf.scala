package com.orch.master

object IPerf {

    import com.orch.utils.{Path, Exe}

    def startServer (port : Int) {
        new Exe (Seq ("iperf", "-s", "-p", "" + port), Path (".")).start ();      
    }

    def launchPerf (addr : String, port : Int) : Int = {
        // val exe = new Exe (Seq ("iperf", "-c", addr, "-p", "" + port, "-t", "1", "--format", "K"), Path ("."));
        // val (code, out, err) = exe.synchro ();
        // var index = out.indexOf (" KBytes/sec") - 1
        // var res = ""
        // while (out (index) != ' ') {
        //     res = out (index) + res
        //     index -= 1
        // }
        // return res.toInt
        return 10000000
    }

}
