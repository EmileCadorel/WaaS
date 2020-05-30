package com.orch.leader.workflow

object WFileType {
    val TRANSITION = 0
    val INPUT = 1
    val OUTPUT = 2
}

/**
  * Representation of a file composing a workflow
  * Params : 
  * - name, the name of the file
  * - size, the size of the file in kbytes
  * - creator, the id of the WTask that create the file
  * - deps, the ids of the WTasks, that require this file
  */
class WFile (val name : String, val path : String, val size : Long, val io : Int, var creator : Long, var deps : Array [Long]) {}
