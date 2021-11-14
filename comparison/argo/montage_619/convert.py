from execo import *
from execo_g5k import *
import yaml
import datetime
from shutil import copyfile 
import ntpath
import yaml
import os
import sys
import random
import argparse


def createArgumentParser ():
    parser = argparse.ArgumentParser ()
    parser.add_argument ("flow", help="flow to convert")
    parser.add_argument ("--out", help="out file", default="out.yaml")
    
    return parser.parse_args ()


def listElements (flowFile) :
    with open (flowFile, "r") as fp :
        content = yaml.load (fp, Loader=yaml.FullLoader)
        return content ["tasks"]

def hasOutput (task, files) :
    if "output" in task : 
        for o in task ["output"] :
            if o in files :
                return True
    return False
    
def retreiveTree (tasks) :
    tree = {}
    for i in range (0, len (tasks)) :
        tree [i] = []
        for j in range (0, len (tasks)) :
            if i != j :
                if "input" in tasks [i] : 
                    if hasOutput (tasks [j], tasks [i] ["input"]) :
                        tree [i] = tree [i] + [j]
    return tree


def createTemplate (task):
    out = {}
    out ["name"] = task ["app"]
    out ["inputs"] = {"parameters" : [{"name" : "cmd_line"}]}
    out ["container"] = {"image" : task ["os"], "command" : ["bin/bash", "-c", "cd /mnt/vol ; export PATH=$PATH:/mnt/vol:. ; ./" + task ["app"] + " {{inputs.parameters.cmd_line}}"]}
    out ["container"]["volumeMounts"] = [{"name" : "workdir", "mountPath" : "/mnt/vol/"}]

    return out

def createTask (tasks, tree, i) :
    task = tasks [i]
    out = {}
    out ["name"] = task ["app"] + str (i)
    out ["template"] = task ["app"]
    out ["arguments"] = {"parameters" : [{"name" : "cmd_line", "value" : task ["params"]}]}
    if len (tree [i]) != 0 :
        deps = []
        for j in tree [i] :
            deps = deps + [tasks [j]["app"] + str (j)]
        out ["dependencies"] = deps
    
    return out

def createFlow (templates, dag) :
    out = {"apiVersion" : "argoproj.io/v1alpha1", "kind" : "Workflow", "metadata" : {"generateName" : "montage"}}
    out ["spec"] = {"entrypoint" : "montage", "volumes" : [{"name" : "workdir", "persistentVolumeClaim" : {"claimName" : "task-pv-claim"}}]}
    
    out ["spec"]["templates"] = []
    for t in templates : 
        out["spec"] ["templates"] = out ["spec"]["templates"] + [templates [t]]
        
    out["spec"] ["templates"] = out["spec"]["templates"] + [{"name" : "montage", "dag" : {"tasks" : dag}}]
    return out

def createArgoWorkflow (tasks, tree) :
    templates = {}
    dag = []
    for i in range (len (tasks)) :
        t = tasks [i]
        if not t ["app"] in templates :
            templates [t ["app"]] = createTemplate (t)
        dag = dag + [createTask (tasks, tree, i)] 

    flow = createFlow (templates, dag)
    return flow
    

    
def main (args) :
    tasks = listElements (args.flow);
    tree = retreiveTree (tasks)
    flow = createArgoWorkflow (tasks, tree)
    dump = yaml.dump(flow, default_flow_style = False, allow_unicode = True, encoding = None)
    with open (args.out, "w") as fs:
        fs.write (dump)
    

if __name__ == "__main__":
    main (createArgumentParser ())
