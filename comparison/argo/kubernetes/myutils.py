#!/usr/bin/env python3

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
from glob import glob
import time

## Known information about nantes clusters
known_roots = {"ecotype" : "172.16.193.", "econome" : "172.16.192.", "chetemi" : "172.16.37."}
known_caps = {"ecotype" : {"cpus" : 20, "memory" : 32768}, "econome" : {"cpus" : 16, "memory" : 16000}}
known_speed = {"ecotype" : 1800, "econome" : 2200}
known_ends = {"chetemi" : ".lille.grid5000.fr", "ecotype" : ".nantes.grid5000.fr", "econome" : ".nantes.grid5000.fr"}

# Return the addr (ip address), and cluster name of the node
# -> (addr, cluster)
def getAddrOfNode (node) :
    cluster = ""
    addr = node.address
    cluster = addr.split (".")[0].split ("-")[0]
    res = known_roots [cluster] + (addr.split (".")[0].split ("-")[1])
    return res

def getNodeFromAddr (node) :
    for i in known_roots :
        if node.find (known_roots [i]) != -1 :
            return i + "-" + node [node.rfind (".") + 1:] + known_ends [i]
    return ""

def getCluster (node):
    cluster = ""
    addr = node.address
    cluster = addr.split (".")[0].split ("-")[0]
    return cluster

def getNodes (started) :
    jobs = get_current_oar_jobs (["nantes", "lille"])
    logger.info ("Current job : " + str (jobs))

    while True: 
        running_jobs = [ job for job in jobs if get_oar_job_info (*job).get ("state") == "Running" ]
        if (len (running_jobs) != 0) : 
            nodes = sorted ([job_nodes for job in running_jobs for job_nodes in get_oar_job_nodes (*job)], key=lambda x: x.address)
            logger.info ("Will deploy on : " + str (nodes))

            deployed, undeployed = deploy (Deployment (nodes, env_name="ubuntu2004-x64-min"), check_deployed_command=started)
            return nodes
        else :
            time.sleep (1)

def launchAndWaitCmd (nodes, cmd, user="root"):
    conn_params = {'user': user}
    cmd_run = Remote (cmd, nodes, conn_params)
    logger.info ("Launch " + cmd)
    cmd_run.start ()
    status = cmd_run.wait ()
    logger.info ("Done")
    
def launchCmd (nodes, cmd, user="root") : 
    conn_params = {'user': user}
    cmd_run = Remote (cmd, nodes, conn_params)
    logger.info ("Launch " + cmd + " on " + str (nodes))
    cmd_run.start ()
    return cmd_run

def waitCmds (cmds):
    for c in cmds :
        c.wait ()
        
def waitCmdsWithOut (cmds, vm_name):
    result = {}
    for c in cmds :
        c.wait ()
        result [vm_name [c._hosts [0]]] = c.processes [0].stdout
    return result

def uploadFile (nodes, f, d):
    conn_params = {'user': 'root'}
    cmd_run = Put (nodes, [f], d, connection_params=conn_params)
    logger.info ("Upload file " + str (f))
    cmd_run.run ()
    cmd_run.wait ()
    logger.info ("Done")    

def downloadFile (nodes, f, d):
    conn_params = {'user': 'root'}
    cmd_run = Get (nodes, [f], d, connection_params=conn_params)
    logger.info ("Download file " + str (f))
    cmd_run.run ()
    cmd_run.wait ()
    logger.info ("Done")        
