package com.orch.leader.scheduling

import com.orch.leader.workflow._

/**
  * An STask associate a WTask to a location
  * This location is stored in a SVM
  * An STack is generated by a scheduler, and is used to generate a CTask
  * The /nid/ and /vid/ are the reference to the node and the vm that is containing the task
  */
class STask (val nid : String, val vid : Long, val begin : Long, val end : Long, val needs : Map [String, Long], val ref : WTask) {}
