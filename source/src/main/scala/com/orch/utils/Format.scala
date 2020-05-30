package com.orch.utils

import scala.reflect.ClassTag
import scala.reflect._

case class Log (date : String, log : String)

object Formatter {

    var lastLine : String = ""

    def center (str : String, size : Int, sep : Char) : String = {
        val buf = new StringBuilder
        val bef = (size - str.length) / 2
        val af  = size - bef - str.length
        for (i <- 1 to bef)
            buf += sep
        buf ++= str
        for (i <- 1 to af)
            buf += sep
        buf.toString
    }

    def createTable [X : ClassTag] (list : Seq[X]) : String = {
        if (list.length == 0) return createHeader[X]
        var max : Map [String, Int] = Map ()
        var buf = new StringBuilder
        var fields = classTag[X].runtimeClass.getDeclaredFields
        for (x <- list) {
            for (f <- fields) {
                val name = f.getName
                f.setAccessible(true)
                var val_len = f.get(x).toString.length
                f.setAccessible(false)
                val_len = if (val_len > name.length) val_len else name.length
                if (!max.contains(name))
                    max += (f.getName -> (val_len + 2))
                else if (max (name) < val_len + 2) {
                    max -= (f.getName)
                    max += (f.getName -> (val_len + 2))
                }
            }
        }

        buf ++= createSeparationLine [X] (max, '╔', '╦', '╗') + '\n'
        buf ++= "║"
        fields.map{ f => buf ++=  center (f.getName, max (f.getName), ' ') + "║" }
        buf ++= "\n"
        buf ++= createSeparationLine[X] (max, '╠', '╬', '╣', '─') + '\n'
        for (x <- list) {
            buf ++= "║"
            for (f <- fields) {
                f.setAccessible(true)
                buf ++= center (f.get (x).toString, max (f.getName), ' ') + "║"
                f.setAccessible(false)
            }
            buf ++= "\n"
        }
        buf ++= createSeparationLine[X] (max, '╚', '╩', '╝') + '\n'
        buf.toString
    }

    def createHeader[X : ClassTag] () : String = {
        val buf = new StringBuilder
        var max : Map [String, Int] = Map ()
        classTag[X].runtimeClass
            .getDeclaredFields
            .map{ f =>
                buf ++= " " + f.getName + " ║"
                val len = 2 + (f.getName).length
                max += (f.getName -> len)
            }
        val aux = new StringBuilder
        aux ++= createSeparationLine[X] (max, '╔', '╦', '╗') + '\n'
        aux ++= '║' + buf.toString + '\n'
        aux ++= createSeparationLine[X] (max, '╠', '╬', '╣', '─') + '\n'
        aux ++= createSeparationLine[X] (max, '╚', '╩', '╝') + '\n'
        aux.toString
    }

    def createSeparationLine[X : ClassTag] (max : Map [String, Int], beg : Char, mid : Char, end : Char, sep : Char = '═') : String = {
        val buf = new StringBuilder
        buf += beg
        var i = 0
        var fields = classTag[X].runtimeClass.getDeclaredFields
        for (z <- fields) {
            i += 1
            buf ++= center ("", max (z.getName), sep)
            if (i != fields.length)
                buf += mid
            else buf += end
        }
        buf.toString
    }

    def createSimpleLine (max : Int, beg : Char, mid : Char, end : Char, sep : Char = '═') : String = {
        val buf = new StringBuilder
        buf += beg
        var i = 0
        buf ++= center ("", max, sep)
        buf += end
        buf.toString
    }


    def createString (str : String) : String = {
        val content = str.split ("\n")
        var max = 0
        for (c <- content) {
            if (c.length > max) max = c.length
        }

        val aux = new StringBuilder
        aux ++= createSimpleLine (max, '╔', '╦', '╗') + '\n'
        for (c <- content)
            aux ++= '│' + c + '│' + '\n'
        aux ++= createSimpleLine (max, '╚', '╩', '╝') + '\n'
        aux.toString
    }

    def printForwardLine (line : String) : Unit = {
        for (x <- 0 to lastLine.length)
            print ("\b");

        for (x <- 0 to lastLine.length)
            print (" ");

        for (x <- 0 to lastLine.length)
            print ("\b");
        print (line)

        lastLine = line
    }

}

