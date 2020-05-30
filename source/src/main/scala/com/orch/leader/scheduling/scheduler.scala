package com.orch.leader.scheduling

import com.orch.common._
import com.orch.leader.workflow._
import com.orch.leader.election._
import com.orch.leader.configuration._

abstract class Scheduler (elector : ClusterElector) {

    /*
     *  The estimated time needed to perform the scheduling and send the orders
     */
    val DELTA = 3; 

    /**
      * When the scheduler is created for the first time, we set the current time to 0
      */
    var zeroInstant : Long = System.currentTimeMillis () / 1000;

    /**
      * The last index used when creating a VM
     */
    var __vmsIDs__ : Long = 0;

    /**
      * The compute nodes on each cluster
      * Each cluster have a homogeneous bandwidth given by bwCluster
      */
    var nodes : Map [String, Array [SNode]] = Map ()

    /**
      * The of all nodes, without taking care of clustering
      */
    var allNodes : Map [String, SNode] = Map ()

    /**
      * The bandwidth of each cluster (internal bandwidth)
      */   
    var bwCluster : Map [String, Int] = Map ()

    /**
      * The bandwidth of the network, i.e. the bandwidth between the cluster
      */
    var bwNetwork : Map [String, Map [String, Int]] = Map ()

    /**
      * The known OS, and there relative boot time
      * The boot time is dependant of the node
      */
    var knownOS : Map [String, Map [String, Long]] = Map ()    

    /**
      * Change the bandwidth inside the cluster /cluster/
      */
    final def informBwCluster (cluster : String, bw : Int) : Unit = {
        bwCluster = bwCluster + (cluster-> bw)
    }

    final def informBwNetwork (A : String, B : String, bw : Int) : Unit = {
        var mpA : Map [String, Int] = if (bwNetwork.contains (A)) bwNetwork (A) else Map ()
        var mpB : Map [String, Int] = if (bwNetwork.contains (B)) bwNetwork (B) else Map ()

        mpA = mpA + (B-> bw)
        mpB = mpB + (A-> bw)

        bwNetwork = bwNetwork + (A-> mpA)
        bwNetwork = bwNetwork + (B-> mpB)
    }

    /**
      * Information about the boot time of the VM in the cluster /cluster/
      */
    final def informBootTimes (node : String, oss : Map [String, Long]) : Unit = {
        for (os <- oss) {
            var current : Map [String, Long] =
                if (knownOS.contains (os._1)) knownOS (os._1)
                else Map ()
            current = current + (node-> (os._2))
            knownOS = knownOS + (os._1-> current)
        }
    }

    /**
      * When a new node is connected to a master, we add it to the cluster
      * This information is mandatory to perform the scheduling
      */
    final def addNewNode (cluster : String, node : Node) : Unit = {
        var list : Array [SNode] = if (nodes.contains (cluster)) nodes (cluster) else Array ()
        val snode = new SNode (node.n_id, cluster, node.capas, node.speed)
        list = list :+ snode

        nodes = nodes + (cluster-> list)
        allNodes = allNodes + (node.n_id -> snode)
    }

    /**
      * A cluster elector will treat the arrival of a new workflow
      * And elect the clusters that are able enough interesant to
      * consider when scheduling the workflow
      * The scheduler will then create the meta cluster that combines
      * all the nodes, and heterogeneous bandwidth
      */
    final def electClusters (flow : Workflow) : List [String] = {
        elector.runElection (flow, nodes, bwNetwork, bwCluster)
    }

    /**
      * Get all the node of the meta clusters
      * A meta cluster gather multiple cluster that are elected by a ClusterElector
      */
    final def createClusterFromElected (clusters : List [String]) : Array [SNode] = {
        var array : Array[SNode] = Array ()
        for (x <- clusters) {
            array = array ++ nodes (x)
        }
        array
    }

    /**
      * When a new workflow is submitted we need to schedule it
      * 3. steps : 
      * - Create a cluster
      * - Schedule on that cluster
      * - Create a report, and return that report
      */
    final def onScheduling (flow : Workflow) : Report = {
        val clusters = electClusters (flow)
        val metaCluster = setToTime (createClusterFromElected (clusters))
        val metaClusterUpdated = schedule (flow, metaCluster)
        if (metaClusterUpdated.length != 0) {
            for (n <- metaClusterUpdated)
                println (n.name + " " + n.prettyPrint ());

            updateGlobalPlan (clusters, metaClusterUpdated)
            val config = createReport (flow, clusters)
            config
        } else // If the scheduling has failed, we can imagine to inform the client, but for the moment I don't care
              new Report (Map (), Map (), Map ())
    }

    /** 
      * Method that needs to be overriden
      * Different for each type of scheduler
      * Returns: the list of updated nodes, with the tasks scheduled in them
      */
    def schedule (flow : Workflow, nodes : Array [SNode]) : Array [SNode];

    /**
      * Update the global plan
      * It means update the information about these nodes inside the scheduler for future scheduling
      * We suppose all the nodes of a cluster are present in the list nodes
      * i.e. \/ i, \in clusters, \/ x \in this.nodes (i), E j \in nodes, j == x
      */
    final def updateGlobalPlan (clusters : List [String], nodes : Array [SNode]) : Unit = {
        for (c <- clusters) {
            println (c)
            val array = nodes.filter ((x) => x.cluster == c)
            this.nodes = this.nodes + (c -> array)
            for (n <- array)
                allNodes = allNodes + (n.name-> n)
        }
    }

    /**
      * Create a report, for each cluster in clusters
      */
    final def createReport (flow : Workflow, clusters : List [String]) : Report = {
        var vms : Map [String, Map [Long, CVM]] = Map ()       
        var tasks : Map [String, Map [String, CTask]] = Map ()
        var files : Map [String, Map [String, Array [CFile]]] = Map ()
        for (c <- clusters) {
            vms = vms + (c -> Map ())
            tasks = tasks + (c -> Map ())
            files = files + (c -> Map ())
        }

        for (t <- flow.tasks) {
            val loc = t._2.getAssociation ()
            val node = allNodes (loc.nid)
            var c = vms (node.cluster)
            c = c + (loc.vid -> toCVM (node.getVM (loc.vid)))
            vms = vms + (node.cluster -> c)

            val (ctask, cfiles) = toCTask (flow, t._2, node)
            var tc = tasks (node.cluster)
            tc = tc + ((ctask.wid + "/" + ctask.id) -> ctask)

            var fc = files (node.cluster)
            for (file <- cfiles) {
                var array : Array [CFile] = if (fc.contains (file._1)) fc (file._1) else Array ()
                array = array ++ file._2
                fc = fc + (file._1 -> array)
            }

            tasks = tasks + (node.cluster -> tc)
            files = files + (node.cluster -> fc)
        }
        new Report (vms, tasks, files)
    }

    /**
      * Create a CVM from a SVM
      */
    final def toCVM (v : SVM) : CVM = {
        CVM (v.nid, v.id, v.os, v.user, v.getBegin (), v.getCapacities (), CVMState.OFF)
    }

    final def toCTask (flow : Workflow, t : WTask, node : SNode) : (CTask, Map [String, Array [CFile]]) = {
        var files : Map [String, Array [CFile]] = Map ()
        for (fId <- t.inputs) {
            val f = flow.files (fId)
            var array : Array [CFile] = if (files.contains (f.name)) files (f.name) else Array ()
            if (f.io == WFileType.INPUT) {
                array = array :+ toCFile (f, flow.id, t.id, flow.remote, "", true)
            } else {
                array = array :+ toCFile (f, flow.id, t.id, node.cluster, node.name, true)
            }

            files = files + ((flow.id + "/" + f.name) -> array)
        }

        for (fId <- t.outputs) {
            val f = flow.files (fId)
            var array : Array [CFile] = if (files.contains (f.name)) files (f.name) else Array ()
            for (sId <- f.deps) {                
                val succ = flow.tasks (sId)
                val succNode = this.allNodes (succ.getAssociation ().nid)
                // We add the file dependance, to the other clusters
                if (succNode.cluster != node.cluster)
                    array = array :+ toCFile (f, flow.id, succ.id, succNode.cluster, succNode.name, false, true)
            }
            if (f.io == WFileType.OUTPUT)
                array = array :+ toCFile (f, flow.id, -1, flow.remote, "", false, false)
            files = files + ((flow.id + "/" + f.name) -> array)
        }

        val ctask = CTask (flow.remote, flow.id, t.id, t.app, t.params, t.getAssociation ().begin, t.getAssociation ().vid, t.outputs)
        (ctask, files)
    }

    final def toCFile (file : WFile, flowId : String, taskId : Long, cluster : String, node : String, input : Boolean, trans : Boolean = false) : CFile = {
        CFile (cluster,
            node,
            file.name,
            file.path,
            if (file.io == WFileType.TRANSITION || trans)
                CFileType.TRANSITION
            else if (file.io == WFileType.INPUT)
                CFileType.INPUT
            else CFileType.OUTPUT,
            flowId, 
            if (input)
                taskId
            else -1
        )
    }

    /**
      * Validate the location /location/ for the task /task/ on the vm /vm/ on the node /node/
      * Returns a new node with the task added
      */
    final def validateLocation (task : WTask, location : STask, vm : SVM, node : SNode) : SNode = {
        task.setAssociation (location)
        val nvm = vm.updateByAdding (location)
        val nnode = node.updateByAdding (nvm)
        nnode
    }

    /**
      * Get the clock time of the scheduler, to be in accordance to the leader
      */
    final def getZeroInstant () : Long = {
        this.zeroInstant
    }

    /**
      * Change the clock time of the scheduler, to be in accordance to the leader
      */
    final def setZeroInstant (zero : Long) : Unit = {
        this.zeroInstant = zero
    }

    /**
      * Get the current instant of the scheduler
      * Relative instant from zeroInstant (the creation of the scheduler)
      */
    final def getCurrentTime () : Long = {
        return (System.currentTimeMillis () / 1000) - zeroInstant;
    }

    /**
      * Update all the node, in order to remove all old information
      * Old VMs are off, and we can't use them anymore
      */
    final def setToTime (nodes : Array [SNode]) : Array [SNode] = {
        var res : Array [SNode] = Array ()
        for (x <- nodes)
            res = res :+ x.removeAllOldVM (getCurrentTime ())
        res
    }

    /**
      * Calculate the time needed by the communication from the node
      * /from/ to the node /to/ If the two nodes are in the same
      * cluster the communication is based on the bandwidth between
      * inside the cluster Else, the communication is based on the
      * three bandwidth, cluster A, A -> B, cluster B.
      */
    final def getComTime (from : SNode, to : SNode, size : Long) : Long = {
        if (from.cluster == to.cluster) {
            return size / bwCluster (from.cluster)
        } else {
            val inA = size / bwCluster (from.cluster)
            val AtoB = size / bwNetwork (from.cluster)(to.cluster)
            val inB = size / bwCluster (to.cluster)
            return inA + AtoB + inB
        }
    }

    /**
      * Returns : the max communication time for predecessors of a task to a node
      * Params: 
      * to = the node to which to files will be sent
      * from = A list of predecessors of a task ._1 = size of the input data
      *                                         ._2 = the task
      */
    final def getMaxComTime (to : SNode, from : List [(Long, WTask)]) : Long = {
        var end : Long = 0
        for (anc <- from) {
            val assoc = anc._2.getAssociation ()
            val loc_end = getComTime (allNodes (assoc.nid), to, anc._1) + assoc.end
            if (loc_end > end)
                end = loc_end
        }
        end
    }

    /**
      * Create a new VM for the node /node/, and the user /user/ with the os /os/
      */
    final def createNewVM (os : String, user : String, node : SNode) : SVM = {
        val boot = if (knownOS.contains (os)) {
            if (knownOS (os).contains (node.name))
                knownOS (os) (node.name)
            else 30 // The os is not bootable on that cluster, but ok for debug purpose
        } else 30 // Ugly but useful is we have no information about the os, just for debug purpose

        __vmsIDs__ += 1;
        new SVM (node.name, __vmsIDs__, os, user, boot)
    }

    final def error (msg : String) : Unit = {
        println ("Failed to scheduler : " + msg)
    }

}


