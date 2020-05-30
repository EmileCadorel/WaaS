package com.orch.leader.workflow

/**
  * Representation of a workflow  
  * **Remote** is the master node that contains the data of the workflow
  */
class Workflow (val id : String, val remote : String, val user : String, val tasks : Map [Long, WTask], val files : Map [String, WFile], val custom : Map [String, Long]) {

    val entries = constructEntries ()

    var outputFile = constructOutputs ()

    /**
      *  Create a subset of /tasks/, that contains only the entry tasks
      */
    def constructEntries () : Map [Long, WTask] = {
        var entries : Map [Long, WTask] = Map ()
        for (t <- tasks) {
            if (t._2.isEntry (files)) entries += (t._1-> t._2)
        }
        entries
    }

    /**
      * Return the list of output files
      */
    def constructOutputs () : Map [String, WFile] = {
        this.files.filter (_._2.io == WFileType.OUTPUT)
    }


    def removeOutputFile (name : String) : Unit = {
        this.outputFile = this.outputFile.- (name)
    }

    /**
      * A workflow if considered finished if all its outputFile list is empty
      */
    def isFinished () : Boolean = {
        outputFile.isEmpty
    }

    /**
      * The list of entry tasks of the workflow
      */
    def getEntries () : Map [Long, WTask] = {
        entries
    }

    /**
      * Return the list of successor from the task /task/
      * ._1 = the size of data from /task/ to the succ
      * ._2 = the successor
      */
    def getSuccessor (task : WTask) : List [(Long, WTask)] = {
        var succ : Map [Long, (Long, WTask)] = Map ()
        for (file <- task.outputs) {
            for (d <- files (file).deps) {
                if (d != 0) {
                    if (!succ.contains (d))
                        succ = succ + (d-> (file.size, tasks (d)))
                    else
                        succ = succ + (d-> (succ (d)._1 + file.size, tasks (d)))
                }
            }
        }
        succ.values.toList
    }

    /**
      * Return a list of predecessors from the task /task/
      * ._1 = the size of data from the predecessor to /task/
      * ._2 = the predecessor
      */
    def getPredecessor (task : WTask) : List [(Long, WTask)] = {
        var pred : Map [Long, (Long, WTask)] = Map ()
        for (file <- task.inputs) {
            val p = files (file).creator
            if (p != 0) {
                if (!pred.contains (p))
                    pred = pred + (p-> (file.size, tasks (p)))
                else
                    pred = pred + (p-> (pred (p)._1 + file.size, tasks (p)))
            }
        }
        pred.values.toList
    }

    def getCustom (elem : String) : Long = {
        return custom (elem)
    }

    def prettyPrint () : String = {
        var ret = "";
        ret = "{\n";
        for (x <- tasks) {
            ret += ("\t" + x._2.prettyPrint () + "\n");
        }        
        ret + "}"
    }

}
