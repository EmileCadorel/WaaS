package com.orch.leader.configuration


object CTaskState {
    val NONE = 0
    val HAS_EXEC = 1
    val RUNNING = 2
}

/**
  * A task information for a configuration
  * - cluster = the cluster where the app has been uploaded
  */
case class CTask (val cluster : String, val wid : String, val id : Long, var app : String, val params : String, val start : Long, val vid : Long, val outs : Array [String], var state : Long = CTaskState.NONE) {}
