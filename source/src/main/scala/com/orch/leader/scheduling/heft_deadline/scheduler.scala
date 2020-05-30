package com.orch.leader.scheduling.heft_deadline

import com.orch.leader.scheduling._
import com.orch.leader.election._
import com.orch.leader.workflow._

class HeftDeadlineScheduler (elector : ClusterElector) extends Scheduler (elector) {

    def schedule (flow : Workflow, nodes : Array [SNode]) : Array [SNode] = {

        val priorities = createPriorityList (flow, flow.getCustom ("deadline"), nodes);
        priorities.foreach (x => print (flow.tasks (x._1).app + " " + x))
        println ("")


        val zero = getCurrentTime () + DELTA;

        var ons = nodes.filter (_.getVMs ().size != 0)
        var offs = nodes.filter (_.getVMs ().size == 0)
        var i = 0;
        val (succ, locNodes) = scheduleBacktrack (0, 0, priorities, flow, ons, offs, zero)

        if (!succ) {
            return Array ()
        } else {
            return locNodes
        }
    }

    def scheduleBacktrack (id : Int, fst : Int, priorities : List [(Long, (Long, Long))], flow : Workflow, ons : Array [SNode], offs : Array [SNode], zero : Long) : (Boolean, Array[SNode]) = {
        println (id+ " " + fst)
        if (priorities.size == 0) {
            return (true, ons ++ offs)
        }

        val f = priorities (0)
        val Q = priorities.slice (1, priorities.length)
        val (succ, loc_ons, loc) = scheduleTask (flow, flow.tasks (f._1), ons, zero)

        if (succ && (loc.end - zero) < f._2._2) {
            val (success, nodes) = scheduleBacktrack (id, fst + 1, Q, flow, loc_ons, offs, zero)
            if (success) return (true, nodes)
        }

        if (id == fst) {
            val (succ, loc_nodes, loc) = scheduleTask (flow, flow.tasks (f._1), ons ++ offs, zero)
            val loc_ons = loc_nodes.filter (_.getVMs ().size != 0)
            val loc_offs = loc_nodes.filter (_.getVMs ().size == 0)
            return scheduleBacktrack (id + 1, fst + 1, Q, flow, loc_ons, loc_offs, zero)
        } else {
            return (false, Array ())
        }
    }


    /**
      * Schedule a task on the nodes
      * Return a list of modified nodes, or an empty list if failure
      */
    def scheduleTask (flow : Workflow, task : WTask, nodes : Array [SNode], zero : Long) : (Boolean, Array [SNode], STask) = {
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
            (true, nodes, usedSTask)
        } else {
            (false, Array (), null)
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
    def createPriorityList (flow : Workflow, deadline : Long, nodes : Array[SNode]) : List [(Long, (Long, Long))] = {
        var priorities : Map [Long, (Long, Long)] = Map ()
        for (t <- flow.getEntries ()) {
            val (d, r, loc_prio) = computeRank (flow, t._2, deadline, nodes, priorities)
            priorities = loc_prio
        }
        priorities.toList.sortWith (_._2._1 > _._2._1)
    }

    /**
      * Compute the rank of a task
      * Its rank is based on the rank of its successors
      */
    def computeRank (flow : Workflow, t : WTask, deadline : Long, nodes : Array[SNode], priorities : Map [Long, (Long, Long)]) : (Long, Long, Map [Long, (Long, Long)]) = {
        if (priorities.contains (t.id)) {
            val len = t.average
            var mean_len : Long = 0;
            for (n <- nodes)
                mean_len = mean_len + (t.getLen() / n.speed);
            mean_len = mean_len / nodes.size;

            (priorities (t.id)._2 - mean_len, priorities (t.id)._1, priorities)
        } else {
            var local_priorities = priorities
            var max : Long = 0
            var max_D : Long = deadline
            for (s <- flow.getSuccessor (t)) {
                val (s_dead, s_rank, s_priorities) = computeRank (flow, s._2, deadline, nodes, local_priorities)
                local_priorities = s_priorities
                var mean_com : Long = 0;
                for (n <- nodes) for (m <- nodes) {
                    val current = getComTime (n, m, s._1)
                    mean_com += current
                }

                mean_com /= (nodes.size * nodes.size)
                if ((s_dead - mean_com) < max_D) max_D = s_dead - mean_com;
                if ((s_rank + mean_com) > max) max = s_rank + mean_com;
            }

            val len = t.average
            var mean_len : Long = 0;
            for (n <- nodes)
                mean_len = mean_len + (t.getLen() / n.speed);
            mean_len = mean_len / nodes.size;

            local_priorities = local_priorities + (t.id-> (max + len, max_D))
            (max_D - mean_len, max + len, local_priorities)
        }
    }


}
