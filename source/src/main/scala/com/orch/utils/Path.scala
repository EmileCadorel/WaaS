package com.orch.utils

import java.io._

case class Path (file : String)

object Path {

    def build (path : Seq [Path]) : Path = {
        if (path.length != 0) {
            var current = new StringBuilder
            current ++= path (0).file
            for (z <- path.slice (1, path.length)) {
                var add = false
                var trunc = false
                if (z.file.length > 0 && z.file (0) == '/') {
                    if (current.length > 0 && current (current.length - 1) == '/')
                        trunc = true
                } else if ((z.file.length == 0 || z.file (0) != '/') && (current.length == 0 || current (current.length - 1) != '/'))
                    add = true

                if (add) current ++= "/" + z.file
                else if (trunc)
                    current ++= z.file.slice (1, z.file.length)
                else current ++= z.file
            }

            Path (current.toString)
        } else
              Path ("")
    }

    def build (path : Path, str : String) : Path = {
        build (Seq (path, Path (str)))
    }

    def build (path : String, str : String) : Path = {
        build (Seq (Path (path), Path (str)))
    }

    def buildStr (path : Seq [String]) : Path = {
        if (path.length == 1) {
            Path (path (0))
        } else {
            build (Seq (Path (path (0)), buildStr (path.drop (1))))
        }
    }

    def mkdirs (path : Path) {
        val file = new java.io.File (path.file)
        file.mkdirs ()
    }

    def writeFile (path : Path, content : String, append : Boolean = false) : Unit = {
        import java.io.FileWriter
        val fw = new FileWriter (path.file, append)
        fw.write (content)
        fw.close ()    
    }
    
    def readFile (path : Path) : String = {
        import scala.io.Source
        import java.nio.file.{Paths, Files}
        if (Files.exists(Paths.get(path.file))) {
            val buf = Source.fromFile (path.file)
            val str = buf.getLines.mkString
            buf.close
            str
        } else ""
    }

    def copyFile (src : Path, dst : Path) {
        import java.io.{File,FileInputStream,FileOutputStream}
        val src_file = new File(src.file)
        val dst_file = new File(dst.file)
        new FileOutputStream(dst_file) getChannel() transferFrom(
            new FileInputStream(src_file) getChannel, 0, Long.MaxValue )
    }

    def setPermission (path : Path) {
        val exe = new Exe (Seq ("chmod", "777", path.file), Path ("."));
        exe.synchro ();
    }

}
