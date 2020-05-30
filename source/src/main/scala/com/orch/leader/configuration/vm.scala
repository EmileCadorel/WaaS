package com.orch.leader.configuration

object CVMState {
    val OFF = 1
    val BOOTING = 2
    val RUNNING = 3
}

/**
  * A CVM is the configuration information about a VM
  * It is simple information about a VM
  */
case class CVM (val nid : String, val id : Long, val os : String, val user : String, val start : Long, val capas : Map [String, Long], var state : Int) {}
