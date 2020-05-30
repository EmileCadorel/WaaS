package com.orch.leader.scheduling.heft

import com.orch.leader.scheduling._
import com.orch.leader.election._
import com.orch.leader.workflow._

class HeftScheduler (elector : ClusterElector) extends Scheduler (elector) {

    def schedule (flow : Workflow, nodes : Array [SNode]) : Array [SNode] = {
        println ("NB nodes : " + nodes.size)
        val priorities = createPriorityList (flow)
        val zero = getCurrentTime () + DELTA;
        var finalNodes = nodes
        var i = 0
        for (t <- priorities) {
            i += 1
            val (succ, locNodes) = scheduleTask (flow, flow.tasks (t._1), finalNodes, zero)
            if (succ)
                finalNodes = locNodes
            else {
                error (flow.tasks (t._1).prettyPrint ())
                return Array ()
            }
        }
        finalNodes
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

    /**
      *  Create the priority list of the tasks
      *  The returned list is the listed of task id sorted by task priority
      */
    def createPriorityList (flow : Workflow) : List [(Long, Long)] = {
        var priorities : Map [Long, Long] = Map ()
        for (t <- flow.getEntries ()) {
            val (r, loc_prio) = computeRank (flow, t._2, priorities)
            priorities = loc_prio
        }
        priorities.toList.sortWith (_._2 > _._2)
    }

    /**
      * Compute the rank of a task
      * Its rank is based on the rank of its successors
      */
    def computeRank (flow : Workflow, t : WTask, priorities : Map [Long, Long]) : (Long, Map [Long, Long]) = {
        if (priorities.contains (t.id)) {
            (priorities (t.id), priorities)
        } else {
            var local_priorities = priorities
            var max : Long = 0
            for (s <- flow.getSuccessor (t)) {
                val com = s._1;
                val (s_rank, s_priorities) = computeRank (flow, s._2, local_priorities)
                local_priorities = s_priorities
                if (com + s_rank > max) max = s_rank + com
            }

            val len = t.average
            local_priorities = local_priorities + (t.id-> (max + len))
            (max + len, local_priorities)
        }
    }

}
