#!/usr/bin/env python3

from execo import *
from execo_g5k import *
import yaml
from datetime import datetime
from shutil import copyfile
import ntpath
import yaml
import os
import sys
import random
import argparse
from myutils import *
import names
import time
import subprocess

def createArgumentParser ():
    parser = argparse.ArgumentParser ()
    parser.add_argument ("--kill", help="only launch kill all", action="store_true")
    
    return parser.parse_args ()


def createHostFile (nodes):
    masters = "master ansible_host=" + nodes [0].address + " ansible_user=root\n"
    workers = ""
    for i in range (len (nodes[1:])):
        workers = workers + "worker" + str(i) + " ansible_host=" + nodes [i + 1].address + " ansible_user=root\n"
    return "[masters]\n" + masters + "\n[workers]\n" + workers

if __name__ == "__main__":
    args = createArgumentParser ()
    
    nodes = sorted (getNodes (True), key=lambda x: x.address)
    launchAndWaitCmd (nodes, "sudo apt-get update"),

    host = createHostFile (nodes)
    print (host)
    
    if (args.kill) :
        with open ("./kill/hosts", "w") as fp:
            fp.write (host)

        
        os.chdir("./kill")
        subprocess.run(["/bin/bash", "./run.sh"])        
    else:
        with open ("./install/hosts", "w") as fp:
            fp.write (host)

        os.chdir("./install")
        subprocess.run(["/bin/bash", "./run.sh"])
        uploadFile (nodes [0], "../montage_31", "/home/kube/")
    
