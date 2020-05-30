package com.orch.leader.workflow

import java.io._
import com.orch.utils._
import java.util.{Map => JMap, List => JArray}
import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConverters._
import com.orch.leader._
import com.orch.master.MasterProto
import akka.actor.{Props, Actor, ActorSystem, ActorRef, ActorLogging , ActorSelection }
import scala.concurrent._
import ExecutionContext.Implicits.global
// import com.orch.leader.workflow._

object Loader {

    /**
      * Load a workflow information into memory
      * This load is made async, and the /actor/ is informed when the loading is done
      * The message passed to /actor/, is MasterProto.WorkflowLoaded (id : Long, flow : Workflow)
      */
    def unzipFlow (id : String, user : String) : Unit = {
        val path = Path.buildStr (Seq (Global.flow_home, "" + id)).file
        // Tar is stupid and try to untar a remote tar if the path name include an @
        val unzip = new Exe (Seq ("tar", "xf", "" + id + ".tgz", "--force-local"), Path (path));
        println (unzip.synchro ());
    }

    /**
      * Load a flow into memory, from a file which shoud be located in {/path/}/flow.yaml
      * This function is called by loadNewFlow, once the flow zip file has been unziped
      * This is in there, that we send the message WorkflowLoaded to the actor
      */
    def createFlow (actor : ActorRef, remote : String, id : String, user : String, path : String, attributes : Map [String, Long]) : Unit = {
        val flowPath = Path.buildStr (Seq (path, "flow.yaml")).file;
        val yaml = (new Yaml).load (new FileInputStream (new java.io.File (flowPath)));
        val root = yaml.asInstanceOf [JMap[String, Object]].asScala;
        val files = loadFiles (root ("files"));
        val (tasks, ins, outs) = loadTasks (id, root ("tasks"), path);
        val flow = constructFlow (id, remote, user, path, tasks, ins, outs, files, attributes)
        actor ! MasterProto.WorkflowLoaded (id, flow)
    }

    /**
      * Load the files inside a flow configuration
      * The input /root/ is a yaml configuration, that is already pointing to the files
      * Returns : a list of file, (id-> data)
      */
    def loadFiles (root : Object) : Map [String, WFile] = {
        var file_ids : Map [String, WFile] = Map ()
        for (obj <- root.asInstanceOf [JArray[Object]].asScala) {
            val yfile = obj.asInstanceOf [JMap[String, Object]].asScala;
            val name = yfile ("id").asInstanceOf [String]
            val size = yfile ("size").asInstanceOf [Int]
            var path = yfile ("name").asInstanceOf [String]
            var io = WFileType.TRANSITION
            if (yfile.contains ("type")) {
                var io_type = yfile ("type").asInstanceOf [String]
                io = if (io_type == "input") WFileType.INPUT
                else if (io_type == "output") WFileType.OUTPUT
                else WFileType.TRANSITION
            }

            file_ids += (name-> new WFile (name, path, size, io, 0, Array ()))
        }
        file_ids
    }

    /**
      * Load the taks from the flow configuration
      * The input /root/ is a yaml configuration of the tasks
      * Returns : 
      *  -.1, the list of tasks
      *  -.2, the list of inputs of the tasks, (idTask-> list of input names)
      *  -.3, the list of output of the tasks, (idTask-> list of output names)
      */
    def loadTasks (fId : String, root : Object, root_name : String) : (Map[Long, WTask], Map [Long, Array [String]], Map [Long, Array[String]]) = {
        var ins  : Map [Long, Array [String]] = Map ()
        var outs : Map [Long, Array [String]] = Map ()
        var id : Long = 1

        var tasks : Map[Long, WTask] = Map ()
        for (obj <- root.asInstanceOf [JArray[Object]].asScala) {
            val ytask = obj.asInstanceOf [JMap[String, Object]].asScala
            val name = ytask ("app").asInstanceOf [String]
            val len = ytask ("mean_len").asInstanceOf [Int]
            val dev = ytask ("dev_len").asInstanceOf [Int]
            val params = if (ytask.contains ("params")) ytask ("params").asInstanceOf [String] else ""
            val os = ytask ("os").asInstanceOf [String]
            var needs : Map [String, Long] = Map ()
            for (need <- ytask ("needs").asInstanceOf [JMap[String, Object]].asScala)
                needs += (need._1-> (need._2.asInstanceOf [Int]).toLong)

            var input  : Array [String] = Array ()
            var output : Array [String] = Array ()
            for (in <- ytask ("input").asInstanceOf [JArray [String]].asScala) {
                input = input :+ in
            }

            for (out <- ytask ("output").asInstanceOf [JArray [String]].asScala) {
                output = output :+ out
            }

            ins += (id-> input)
            outs += (id-> output)

            tasks += (id-> new WTask (fId, id, name, params, len, dev, os, needs, input, output))
            id += 1
        }
        (tasks, ins, outs)
    }

    /**
      *  Construct the workflow topology from the readed data from the yaml file
      *  Params : 
      *  - id, the id of the workflow
      *  - user, the user owning the workflow
      *  - path, the path of the workflow files
      *  - tasks, the tasks of the workflow
      *  - ins, the input files of each task
      *  - outs, the output files of each task
      *  - files, the files 
      */
    def constructFlow (id : String, remote : String, user : String, path : String, tasks : Map [Long, WTask], ins : Map [Long, Array [String]], outs : Map [Long, Array[String]], files : Map [String, WFile], attributes : Map [String, Long]) : Workflow = {
        var creators : Map [String, Long] = Map ()
        var deps : Map [String, Array [Long]] = Map ()

        for (t <- tasks) {
            for (i <- ins (t._1)) {
                if (deps.contains (i))
                    deps += (i-> (deps (i) :+ t._1))
                else deps += (i-> Array (t._1))                
            }

            for (o <- outs (t._1)) {
                creators += (o-> t._1)                    
            }
        }

        for (f <- files) {
            f._2.creator = if (creators.contains (f._1)) creators (f._1) else 0
            f._2.deps = if (deps.contains (f._1)) deps (f._1) else Array ()
        }

        new Workflow (id, remote, user, tasks, files, attributes)
    }

}
