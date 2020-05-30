package com.orch.leader.scheduling

import scala.math.{min, max}

import com.orch.common._
import com.orch.leader.workflow._
import com.orch.leader.election._
import util.control.Breaks._

/**
  * Set of functions, that can be useful when defining a new scheduler
  * They are not part of a scheduler but provide elementary functionnalities
  */
object SchedulingUtils {

    /**
      * ===============================================
      *               VM Scheduling utils
      * ===============================================
      */

    /**
      * Find a location on the VM that can take a task
      * Params : 
      * needs = the task needs
      * start = the minimal start time of the task
      * zero = the current instant of the scheduler
      * Returns: the instant when we can place the task, -1 if the task cannot fit in the VM
      */
    final def getPlaceOnVm (zero : Long, node : SNode, vm : SVM, start : Long, task : WTask, nVM : Boolean) : Long = {
        val isStarted = vm.getBegin () < zero && !nVM;

        var location = if (isStarted) // The vm has already started
            max (start, max (zero, vm.getBoot () + vm.getBegin ()))
        else // The VM is not even booting
            max (start, zero + vm.getBoot ())
        
        val auxNode = node.removeVM (vm);
        var nb = 0;
        while (true) {
            nb += 1;
            val (time, after, over) = getCollisionOnVM (location, task, vm, isStarted, auxNode);

            // If after, then the location of the task has no impact on the boot instant of the VM
            if (over.begin == -1) {
                return location;
            }
            else if (over.begin == -2) {
                return -1;
            } else if (location > over.end && time) {
                if (after) {
                    return -1; // No impact on the boot instant the collision is with a VM that is after this one
                }
                location = over.end + 1 + vm.getBoot ()                
            } else (location = over.end + 1)
        }
        return -2
    }

    /**
      * Get the collision created by a VM when scheduling a new task on it
      * Params : 
      * loc = the instant when we want to start the task
      * task = the task that we want to place on the VM
      * vm = the Vm that will be used to execute the task
      * isStarted = is the VM started
      * node = the node in which the vm is located
      * nodeUsage = the usage of the node, without vm
      */
    final def getCollisionOnVM (loc : Long, task : WTask, vm : SVM, isStarted : Boolean, node : SNode) : (Boolean, Boolean, Interval) = {
        val stask = new STask (node.name, vm.id, loc, (task.getLen () / node.speed) + loc, task.getNeeds (), task)
        val nVM = vm.updateByAdding (stask)
        val isVmValid = isValid (isStarted, vm, nVM.getCapacities ())
        val vmCapasIfAdd = if (!isStarted)
            nVM.getCapacities ()
        else if (isVmValid) vm.getCapacities ()
        else nVM.getCapacities ()

        val over = lastOver (nVM.getBegin (), nVM.getEnd (), vmCapasIfAdd, node.getUsage (), node.getCapacities ())
        if (over.begin == -1 && isVmValid) return (false, false, over)
        else if (diffCapas (vm.getCapacities (), nVM.getCapacities ())) {
            val max_capas = if (!isStarted)
                computeMaxCapas (nVM.getBegin (), nVM.getEnd (), vm.getCapacities (), node.getUsage (), node.getCapacities ())
            else vm.getCapacities ()

            if (max_capas == null) {// The VM can't grow
                return (false, false, new Interval (-2, 0, 0))
            }

            val inner_over = lastOver (loc, loc + (task.getLen () / node.speed), task.getNeeds (), vm.getUsage (), max_capas)
            if (inner_over.begin != -1) {
                return (false, false, inner_over);
            }
        }

        return (true, loc != (nVM.getBegin () + vm.getBoot ()), over)
    }

    /**
      * Find the last interval that overload the capacities /K/, in at least one dimension
      * Could be empty, and return the interval (-1,-1,-1)
      */
    final def lastOver (start : Long, end : Long, add : Map [String, Long], usage : Map [String, Array [Interval]], K : Map [String, Long]) : Interval = {
        var endOver = new Interval (-1, -1, -1)
        for (u <- add) {
            if (usage.contains (u._1)) {
                //breakable {
                for (j <- usage (u._1)) if (j.end >= start) {
                    val x = max (j.begin, start)
                    val y = min (j.end, end);
                    if (x < y) { // If there is a collision, then
                                     // we at least need to place the
                                     // element after the usage that
                                     // is provoking the collision

                        if (endOver.end < j.end && (j.height + u._2) > K (u._1)) {
                            endOver = new Interval (x, j.end, j.height + u._2)
                        }
                        // } else if (j.begin > end) break; // no need to continue everything else is after the element
                    }
                }
            }
        }

        endOver
    }

    /**
      *  Find the first interval that is not full 
      */
    final def findFirstLoc (start : Long, add : Map [String, Long], usage : Map [String, Array [Interval]], K : Map [String, Long]) : Interval = {
        var endOver = new Interval (start, start, -1)
        for (u <- add) {
            if (usage.contains (u._1)) {
                breakable {
                    for (x <- usage (u._1)) {
                        if (x.begin >= start && x.begin > endOver.end) {
                            val intersect = IntervalUtils.intersect (new Interval (start, start + 1, u._2), x)
                            intersect match {
                                case Some (inter : Interval) =>
                                    if (inter.height <= K (u._1)) {
                                        if (inter.end > endOver.end) {
                                            endOver = inter
                                            break
                                        }
                                    }
                                case None => {}
                            }
                        }
                    }
                }
            }
        }
        return endOver
    } 

    /**
      * Tell if the VM would be valid if it had the capacities nCapas
      * Basically, just verify that the didn't grow after it has been booted
      */
    final def isValid (isStarted : Boolean, vm : SVM, nCapas : Map [String, Long]) : Boolean  = {
        if (isStarted) {
            for (x <- nCapas)
                if (vm.getCapacity (x._1) < x._2) return false
        }
        true
    }

    /**
      * Is A diff B
      * Warning : assume the all keys of B are in A
      */
    final def diffCapas (A : Map [String, Long], B : Map [String, Long]) : Boolean = {
        for (x <- A)
            if (!B.contains (x._1) || B (x._1) != x._2) return true
        false
    }

    final def computeMaxCapas (start : Long, end : Long, oldCapas : Map [String, Long], usage : Map [String, Array [Interval]], K : Map [String, Long]) : Map [String, Long] = {
        var res : Map [String, Long] = Map ()
        for (k <- K) {
            if (usage.contains (k._1)) {
                val set = IntervalUtils.subset (usage (k._1), start, end)
                val max = IntervalUtils.maxIntervalOnIntersect (set)
                if (k._2 - max < oldCapas (k._1)) return null
                res += (k._1 -> (k._2 - max))
            } else {
                if (k._2 < oldCapas (k._1)) return null
                res += (k._1 -> k._2)
            }
        }
        res
    }

}
