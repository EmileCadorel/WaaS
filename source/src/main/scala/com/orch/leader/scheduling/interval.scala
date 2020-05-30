package com.orch.leader.scheduling

import scala.math.{min, max}

/**
  *  An interval is a temporal element with a given height
  *  It used to verify collision in resource sharing
  */
class Interval (val begin : Long, val end : Long, val height : Long, val color : Int = 0) {
    val len = end - begin
}

object IntervalUtils {
    import com.orch.utils._

    /**
      * Returns : the interval of collision between this and the interval /i/
      */
    def intersect (i : Interval, j : Interval) : Option [Interval] = {
        val x = max (i.begin, j.begin)
        val y = min (i.end, j.end)
        if (x < y) return Some (new Interval (x, y, i.height + j.height))
        else return None
    }

    def firstIntersect (I : Interval, L : List [Interval]) : (List [Interval], List [Interval], Interval) = {
        var seg = I
        var irest = 0
        var icut = 1
        var toCut : List [Interval] = List (seg)
        var toRest : List [Interval] = List ()
        for (t <- L) {
            val inter = intersect (seg, t)
            inter match {
                case Some (x : Interval) =>
                    toCut = t :: toCut
                    seg = x
                case None =>
                    toRest = t :: toRest
            }
        }
        (toCut, toRest, seg)
    }

    /**
      * Take a list of interval and transform them into a list of consecutive interval with different heights
      * The list of out intervals are sorted by begin, and there is no collision in them
      */
    def allIntersect (L : Array[Interval]) : Array [Interval] = {
        var Qu = L.toList
        var inters : List [Interval] = List ()
        while (Qu.length > 0) {
            val seg = Qu (0)
            val (toCut, toRest, inter) = firstIntersect (seg, Qu.drop (1))
            inters = inter :: inters
            var R = toRest
            for (it <- toCut) {
                val left = new Interval (it.begin, inter.begin , it.height)
                val right = new Interval (inter.end, it.end, it.height)
                if (left.len > 0) R = left :: R
                if (right.len > 0) R = right :: R
            }
            Qu = R
        }

        fusion (inters.sortWith (_.begin < _.begin).toArray)
    }

    def fusion (L : Array[Interval]) : Array [Interval] = {
        var ret : Array [Interval] = Array ()
        var current = new Interval (-1, -1, -1)
        for (x <- L) {
            // if there is only one second of difference, we remove the hole

            if ( (x.begin - current.end).abs <= 1 && x.height == current.height) {
                current = new Interval (current.begin, x.end, current.height)
            } else {
                if (current.begin != -1) {
                    ret = ret :+ current
                }
                current = x
            }
        }
        if (current.begin != -1)
            ret = ret :+ current

        return ret 
    }

    /**
      * Take a list of interval that are computed by the function allIntersect
      * It is assumed that there is no interesction in the list /L/
      */
    def maxIntervalOnIntersect (L : Array [Interval]) : Long = {
        if (L.length != 0) {
            val x = L.reduce ((a, b) => if (a.height < b.height) b else a)
            x.height
        } else 0
    }


    /**
      * Find the first interval that is over max when adding the interval (start, end, need)
      *  Warning : assume L is sorted
      */
    def lastOver (L : Array [Interval], max : Long) : Option [Interval] = {
        for (j <- 0 until L.length) {
            val i = (L.length - j) - 1;
            if (L (i).height > max) return Some (L (i))
        }
        return None
    }

    /**
      * Create a subset of interval between start and end
      */
    def subset (L : Array[Interval], start : Long, end : Long) : Array [Interval] = {
        var res : List [Interval] = List ()
        val ref = new Interval (start, end, 0)
        for (int <- L) {
            val intersect = IntervalUtils.intersect (ref, int)
            intersect match {
                case Some (x : Interval) =>
                    res = x :: res
                case None => {}
            }
        }
        res.toArray
    }

    /**
      * Return the biggest intersection between i and the list L (in term of height)
      */
    def biggest (i : Interval, L : Seq [Interval]) : Interval = {
        var max = i
        for (l <- L) {
            val inter = intersect (i, l)
            inter match {
                case Some (x : Interval) =>
                    if (x.height > max.height)
                        max = x
                case _ => {}
            }
        }
        max
    }

    def prettyFormat (L : Array [Interval]) : String = {
        val intersects = allIntersect (L)
        val n = maxIntervalOnIntersect (intersects).toInt
        var buf = new StringBuilder
        var lines : Array [Array [Interval]] = new Array (n)
        for (i <- 1 to n) lines (i - 1) = Array ()
        var rtype = 1
        var maxEnd : Long = 0
        for (t <- L) {
            var nb = t.height
            var j = 0
            while (nb > 0 && j < lines.length) {
                val inter = biggest (t, lines (j))
                if (inter.height == t.height) {
                    lines (j) = lines (j) :+ new Interval (max (1, t.begin), max (1, t.end), 1, if (t.color == 0) rtype else t.color)
                    nb -= 1
                }
                j += 1
            }

            if (t.end > maxEnd) maxEnd = t.end
            rtype %= 9
            rtype += 1
        }

        buf ++= "╔"
        for (it <- 0.toLong to 3) buf ++= "═"
        buf ++= "╦"
        for (it <- 5.toLong to (maxEnd - 1) * 4 + 8) buf ++= ("═")
        buf ++= "╗\n"
        var i = 0
        for (it <- lines) {
            i += 1
            val line = Formatter.center (s"$i", 4, '.')
            buf ++= "│" + line + "│" + toStr (it, maxEnd) + "│\n"
        }

        buf ++= "│"
        for (it <- 0 to 3) buf ++= " "
        buf ++= "│"
        for (it <- 0.toLong to maxEnd - 1)
            buf ++= s"╷ ${it%10}╷"
        buf ++= "│\n"

        buf ++= "│"
        for (it <- 0 to 3) buf ++= " "
        buf ++= "│"
        for (it <- 0.toLong to maxEnd - 1)
            if (it % 10 == 0)
                buf ++= s"╷ ${it/10%10}╷"
            else
                buf ++= "╷  ╷"

        buf ++= "│\n╚"
        for (it <- 0.toLong to 3) buf ++= "═"
        buf ++= "╩"
        for (it <- 5.toLong to ((maxEnd - 1) * 4 + 8))   buf ++= "═"
        buf ++= "╝\n"
        buf.toString
    }

    def toStr (L : Array [Interval], max : Long) : String = {
        val line = L.sortWith ((a, b) => a.begin < b.begin)
        var buf = new StringBuilder
        var current : Long = 0
        for (t <- line) {
            if (current < t.begin) {
                for (j <- current until t.begin)
                    buf ++= "\033[09m    \033[m"
            }
            buf ++= s"├\u001B[1;4${t.color%10}m"
            val inner = Formatter.center (s"${t.color}", (t.len * 4 - 2).toInt, '═')
            buf ++= inner + "\u001B[0m┤"
            current = t.end
        }

        for (it <- current to max - 1)
            buf ++= "\033[09m    \033[m"
        buf.toString
    }


}



