package com.orch.leader.scheduling.min_min

import com.orch.leader.scheduling._
import com.orch.leader.election._
import com.orch.leader.workflow._

class MinMinScheduler (elector : ClusterElector) extends Scheduler (elector) {

    def schedule (flow : Workflow, nodes : Array [SNode]) : Array [SNode] = {
        var f = flow;
        var finalNodes = nodes
        val zero = getCurrentTime () + DELTA;

        while (f.tasks.size != 0) {
            val (undep, f_bis) = getUndependant (f);
            f = f_bis;

            for (t <- undep.values.toList.sortWith ((x, y) => x.average + (flow.getSuccessor (x).map (_._1).sum)  < y.average + (flow.getSuccessor (y).map (_._1).sum))) {
                val (succ, s_nodes) = scheduleTask (flow, flow.tasks (t.id), finalNodes, zero)
                if (succ)
                    finalNodes = s_nodes
                else {
                    error (flow.tasks (t.id).prettyPrint ())
                    return Array ()
                }
            }
        }
        finalNodes
    }

    def getUndependant (flow : Workflow) : (Map [Long, WTask], Workflow) = {
        val list = flow.getEntries ();
        val rest = flow.tasks.filter (x => !list.contains (x._2.id))
        var real_rest : Map [Long, WTask] = Map () 
        for (t <- rest) {
            var inputs : Array [String] = Array ();
            for (i <- t._2.inputs) {
                if (!list.contains (flow.files (i).creator)) {
                    inputs = inputs :+ i;
                }
            }
            real_rest = real_rest + (t._2.id-> new WTask (t._2.wid, t._2.id, t._2.app, t._2.params, t._2.average, t._2.deviation, t._2.os, t._2.needs, inputs, t._2.outputs));
        }
        (list, new Workflow (flow.id, flow.remote, flow.user, real_rest, flow.files, flow.custom))
    }


    /**
      * Schedule a task on the nodes
      * Return a list of modified nodes, or an empty list if failure
      */
    def scheduleTask (flow : Workflow, task : WTask, nodes : Array [SNode], zero : Long) : (Boolean, Array [SNode]) = {
        val predecessors = flow.getPredecessor (task)

        var used : SNode = null
        var usedVM : SVM = null
        var loc : Long = -1
        var usedSTask : STask = null

        var index = 0
        for (i <- 0 until nodes.length) {
            val (node, vm, stask, localLoc) = scheduleTaskOnNode (flow, task, predecessors, nodes (i), zero)
            if ((loc == -1 || localLoc < loc) && localLoc != -1) {
                loc = localLoc
                used = node
                usedVM = vm
                usedSTask = stask
                index = i
            }
        }
        if (used != null) {
            task.setAssociation (usedSTask)
            usedVM = usedVM.updateByAdding (usedSTask)
            nodes (index) = used.updateByAdding (usedVM)
            println (usedSTask.end)
            (true, nodes)
        } else {
            (false, Array ())
        }
    }

        /**
      * Find a location for the task /task/ on the node /node/ where, the task belongs to /flow/, and its predecessors are /predecessors/
      */
    def scheduleTaskOnNode (flow : Workflow, task : WTask, predecessors : List [(Long, WTask)], node : SNode, zero : Long) : (SNode, SVM, STask, Long) = {

        var used : SVM = null
        var usedLoc : Long = -1
        var usedSTask : STask = null

        val minStart = getMaxComTime (node, predecessors)
        for (vm <- node.getVMs ()) {
            if (vm._2.os == task.os && vm._2.user == flow.user) { // If the VM can run the task
                val loc = SchedulingUtils.getPlaceOnVm (zero, node, vm._2, minStart, task, false)
                if ((usedLoc == -1 || usedLoc > loc) && loc != -1) {
                    usedLoc = loc
                    usedSTask = new STask (node.name, vm._2.id, loc, loc + (task.getLen () / node.speed), task.getNeeds (), task);
                    used = vm._2
                }
            }
        }

        val nVM = createNewVM (task.os, flow.user, node)
        val loc = SchedulingUtils.getPlaceOnVm (zero, node, nVM, minStart, task, true)
        if ((usedLoc == -1 || usedLoc > loc) && loc != -1) {
            usedLoc = loc
            usedSTask = new STask (node.name, nVM.id, loc, loc + (task.getLen () / node.speed), task.getNeeds (), task);
            used = nVM
        }

        if (used != null)
            (node, used, usedSTask, usedLoc)
        else (node, used, usedSTask, usedLoc)
    }

}
