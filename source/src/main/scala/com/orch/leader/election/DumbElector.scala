package com.orch.leader.election

import com.orch.leader.workflow._
import com.orch.leader.scheduling._

/**
  * Dumb elector just return the list of clusters
  * It elect all the clusters
  */
class DumbElector () extends ClusterElector {

    def runElection (flow : Workflow, clusters : Map [String, Array [SNode]], bw : Map [String, Map [String, Int]], bwCluster : Map [String, Int]) : List [String] = {
        clusters.keySet.toList
    }

}
