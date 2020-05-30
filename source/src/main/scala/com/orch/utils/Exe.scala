package com.orch.utils
import com.orch.utils._
import scala.concurrent._
import ExecutionContext.Implicits.global

class Exe(command: Seq[String], path : Path, onExit:(Int, String, String)=>Unit = (x, y, z) => {}, onFailure: (String, Int)=> Boolean = (x, y) => {false}) {

    import scala.sys.process._
    import scala.io._
    import java.io._
    import scala.concurrent._
    import scala.concurrent.duration.Duration
    import scala.concurrent.Future._
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.util.{Success, Failure}

    var future : Thread = null
    var process : Process = null
    var apply : Boolean = true

    def start () : Unit = {
        this.future = new Thread () {
            override def run () : Unit = {
                launchProcess ()
            }
        }
        this.future.start ()
    }

    def synchro () : (Int, String, String) = {
        var out = new StringBuilder
        var err = new StringBuilder
        val process = Process (command, new File (path.file)).run (
            new ProcessIO (
                stdin => {},
                stdout => {Source.fromInputStream(stdout).getLines.foreach(x => {
                    out ++= x + '\n'
                })},
                stderr => Source.fromInputStream(stderr).getLines.foreach(x => {
                    err ++= x + '\n'
                }))
        );

        (process.exitValue (), out.toString, err.toString)
    }


    def launchProcess () : Unit = {
        var end = false
        var nb = 0
        while (!end) {
            var out = new StringBuilder
            var err = new StringBuilder
            this.process = Process(command, new File (path.file)).run(
                new ProcessIO(
                    stdin => {},
                    stdout => {Source.fromInputStream(stdout).getLines.foreach(x => {
                        out ++= x + '\n'
                    })},
                    stderr => Source.fromInputStream(stderr).getLines.foreach(x => {
                        err ++= x + '\n'
                    }))
            );

            val value = process.exitValue ()
            if (apply) {
                if (value == 0) {
                    onExit (value, out.toString, err.toString)
                    end = true
                } else {
                    end = onFailure (err.toString, nb)
                }
            } else end = true
            nb += 1
        }
    }

    def timeout () : Unit = {
        if (this.process != null) {
            apply = false
            this.process.destroy ()
            this.process.exitValue ()
        }
        this.future = null
    }

    def stop () : Unit = {
        if (this.process != null) {
            apply = false
            this.process.destroy ()
            this.process.exitValue ()
        }

        this.future = null
    }

    def await () : Unit = {
        if (this.process != null)
            this.process.exitValue ()
    }
}
