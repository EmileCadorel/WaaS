package com.orch.leader.workflow

import com.orch.leader.scheduling._

/**
  * Representation of a task composing a workflow
  * Params : 
  * - id, the id of the task (generated)
  * - app, the name of the app executed by this task
  * - params, the params to pass at the executable
  * - inputs, the input dependence of the task
  * - outputs, the outputs of the task
  */
class WTask (val wid : String, val id : Long, val app : String, val params : String, val average : Long, val deviation : Long, val os : String, val needs : Map [String, Long], val inputs : Array[String], val outputs : Array [String]) {

    /**
      * The associate is an STask that is constructed at scheduling time
      * It is used to ease the search of the location of the ancestor
      * and successor task when creating the report
      */
    var associate : STask = null

    def isEntry (files : Map [String, WFile]) : Boolean = {
        for (x <- inputs)
            if (files (x).creator != 0) return false;
        true
    }

    def prettyPrint () : String = {
        "(" + id + ", " + app + ", in: " + inputs + ", out: " + outputs + ")"
    }

    /**
      * Schedule time function
      * When validating a location, we must associate this task to its location
      */
    def setAssociation (stask : STask) : Unit = {
        this.associate = stask
    }

    /**
      * Used at scheduling time
      */
    def getAssociation () : STask = {
        this.associate
    }

    def getLen () : Long = {
        average
    }
    
    /**
      * Returns the task needs
      */
    def getNeeds () : Map [String, Long] = {
        needs
    }

}
