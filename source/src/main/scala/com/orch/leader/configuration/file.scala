package com.orch.leader.configuration

object CFileType {
    val TRANSITION = 0
    val INPUT = 1
    val OUTPUT = 2
}

/**
  * A cfile is a file transition between to CTask
  * cluster = the cluster where it need to be sent
  */
case class CFile (val cluster : String, val nid : String, val name : String, val path : String, val io : Long, val wid : String, val deps : Long) {}
