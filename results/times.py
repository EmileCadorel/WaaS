from __future__ import print_function
from datetime import datetime, timedelta
import yaml
import argparse
import os 
import codecs
import sys 
import random
from datetime import datetime

def openFile (in_file) :
    with open (in_file) as stream :
        try:
            return yaml.load (stream)
        
        except yaml.YAMLError as exc:
            None
    return {}


def parseTime (time) : 
    # we specify the input and the format...
    t = datetime.strptime(time, "%H:%M:%S")
    # ...and use datetime's hour, min and sec properties to build a timedelta
    delta = timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)
    return delta

def meanTime (tasks) :
    dico = {}
    for u in tasks :
        t = tasks [u]
        duration = parseTime (t["duration"]).total_seconds ()
        if (t["name"] in dico) :
            dico [t["name"]]["nb"] = dico [t["name"]]["nb"] + 1
            dico [t["name"]]["sum"] = dico [t["name"]]["sum"] + duration
            if (dico [t["name"]]["max"] < duration):
                dico [t["name"]]["max"] = duration
            if (dico [t["name"]]["min"] > duration):
                dico [t["name"]]["min"] = duration
        else :
            dico [t["name"]] = {"nb" : 1, "sum" : duration, "max" : duration, "min" : duration}

    for j in dico :
        dico [j]["mean"] = (dico[j]["sum"])/dico[j]["nb"]
        dico [j]["delta"] = (dico[j]["max"]) - (dico [j]["min"])
    return dico

def meanTimeBootVM (vms) :
    boot = 0
    for u in vms :
        t = vms [u]
        duration = parseTime (t["boot"]).total_seconds ()
        boot += duration
    boot = boot / len (vms)
    return boot

def makespanFlows (flows) :
    tab = []
    for f in flows :
        t = flows [f]

        if ("duration" in t):
            duration = parseTime (t["duration"]).total_seconds ()
            tab = tab + [duration]
    return sorted (tab)
    
    
def main () :
    parser = argparse.ArgumentParser()

    parser.add_argument("log", help="log file")
    args = parser.parse_args ()
    fi = openFile (args.log)
    if ("tasks" in fi):
        print (len (fi["tasks"]))
        times = meanTime (fi["tasks"])
        for i in times :
            print (i, " => ", times [i])

    if ("vms" in fi):
        boot = meanTimeBootVM (fi["vms"])
        print ("Mean boot time : " + boot)
        print ("Nb virtual resource : " + len (fi["vms"]))
        
    if ("workflows" in fi): 
        makespan = makespanFlows (fi["workflows"])
        println ("Makespans : " + str (makespans))
            




    

if __name__ == "__main__" :
    main ()
