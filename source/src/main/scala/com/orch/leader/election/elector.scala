package com.orch.leader.election

import com.orch.leader.workflow._
import com.orch.leader.scheduling._

abstract class ClusterElector () {

    /**
      * Run an election
      * This election is based on whatever criteria you want, for
      * example take the first three cluster that are close to the
      * cluster containing the input file and execs of the flow
      */
    def runElection (flow : Workflow, clusters : Map [String, Array [SNode]], bwNetwork : Map [String, Map [String, Int]], bwCluster : Map [String, Int]) : List [String];

}
