from __future__ import print_function
import yaml
import argparse
import os 
import codecs
import sys 
import random
from datetime import datetime

def registerInfo (msg, date, dico) :
    if (msg.find ("New daemon") != -1) :
        daemon = msg [msg.find ("New daemon")+len ("New daemon"):msg.find("is connected")].strip ()

        if ("daemons" in dico) :
            dico ["daemons"] = dico ["daemons"] + [daemon]
        else :
            dico ["daemons"] = [daemon]

    elif (msg.find ("Start download of flow") != -1):
        index = msg.find (":")
        name = msg [index+1:].strip ()
        date = date [1:-1]
        date = date [:date.find (".")]
        workflow = {name : {"start" : date}}
        if ("workflows" in dico) :
            dico ["workflows"][name] = {"start" : date}
        else :
            dico ["workflows"] = workflow
    elif (msg.find ("Workflow") != -1 and msg.find ("is finished") != -1) :
        name = msg [msg.find ("Workflow") + len ("Workflow"):msg.find ("is finished")].strip ()
        # The workflow is already in the dico
        workflow = dico["workflows"][name]
        date = date [1:-1]
        date = date [:date.find (".")]
        workflow ["end"] = date
        end = datetime.strptime(workflow["end"], '%m/%d/%Y %H:%M:%S')
        start = datetime.strptime(workflow["start"], '%m/%d/%Y %H:%M:%S')
        workflow ["duration"] = str (end - start)
    elif (msg.find ("Starting task") != -1) :
        # 1-master@172.16.193.12_5000/64:/1-master@172.16.193.12_5000/mProject@ecotype-12.nantes.grid5000.fr
        msg = msg [msg.find (":")+1:].strip ()
        workflow, msg = msg [:msg.find("/")].strip (), msg[msg.find ("/")+1:].strip ()
        task, msg = msg [:msg.find (":")].strip (), msg[msg.find (":")+1:].strip ()
        name, msg = msg [:msg.rfind ("@")].strip (), msg[msg.rfind("@")+1:].strip ()        
        name = name[name.rfind("/")+1:]
        taskid = workflow + "/" + str (task)
        print (taskid)
        
        date = date [1:-1]
        date = date [:date.find (".")]
        if ("tasks" in dico) :
            dico ["tasks"][taskid] = {"start" : date, "name" : name, "workflow" : workflow}
        else : 
            dico ["tasks"] = {taskid : {"start" : date, "name" : name, "workflow" : workflow}}
    elif (msg.find ("Task") != -1 and msg.find ("finished")!= -1):

        name = msg [:msg.rfind ("@")].strip ()
        name = name [name.rfind(" ")+1:]
        print (name)
                
        task = dico ["tasks"][name]
        date = date [1:-1]
        date = date [:date.find (".")]
        task ["end"] = date
        end = datetime.strptime(task["end"], '%m/%d/%Y %H:%M:%S')
        start = datetime.strptime(task["start"], '%m/%d/%Y %H:%M:%S')
        task ["duration"] = str (end - start)
    elif (msg.find ("Launch VM") != -1) :
        msg = msg[msg.find (":")+1:]
        vm, node = msg[:msg.find("@")].strip (), msg[msg.find("@")+1:].strip ()
        date = date [1:-1]
        date = date [:date.find (".")]
        if ("vms" in dico) :
            dico ["vms"][vm] = {"start" : date, "node" : node}
        else :
            dico ["vms"] = {vm : {"start" : date, "node" : node}}
    elif (msg.find ("VM") != -1 and msg.find ("is Ready") != -1) :
        msg = msg[len ("VM")+1:msg.find ("is Ready")]
        node, vm = msg[:msg.find("/")].strip (), msg[msg.find("/")+1:].strip ()        
        date = date [1:-1]
        date = date [:date.find (".")]
        vm_ = dico ["vms"][vm]
        vm_ ["ready"] = date
        end = datetime.strptime(vm_["ready"], '%m/%d/%Y %H:%M:%S')
        start = datetime.strptime(vm_["start"], '%m/%d/%Y %H:%M:%S')
        vm_["boot"] = str (end - start)

    elif (msg.find ("VM") != -1 and msg.find ("is Off") != -1) :
        msg = msg[len ("VM")+1:msg.find ("is Off")]
        node, vm = msg[:msg.find("/")].strip (), msg[msg.find("/")+1:].strip ()        
        date = date [1:-1]
        date = date [:date.find (".")]
        vm_ = dico ["vms"][vm]
        vm_ ["end"] = date
        end = datetime.strptime(vm_["end"], '%m/%d/%Y %H:%M:%S')
        start = datetime.strptime(vm_["start"], '%m/%d/%Y %H:%M:%S')
        vm_["duration"] = str (end - start)        
    # else : not important msg
    
def interpretLine (line, dico) :
    # A line is composed of 5 element [INFO] [date] [??] [actor] msg
    info_index = line.find ("]")
    if (info_index != -1) :
        info, line = line [:info_index+1].strip (), line[info_index+1:].strip ()
        date_index = line.find ("]")
        date, line = line [:date_index+1].strip (), line[date_index+1:].strip ()
        ignore_index = line.find ("]")
        ignore, line = line [:ignore_index+1].strip (), line[ignore_index+1:].strip ()
        actor_index = line.find ("]")
        actor, msg = line[:actor_index+1].strip (), line[actor_index+1:].strip ()
                                        
        registerInfo (msg, date, dico)
    

def parseLogFile (log_file) :
    dico = {}
    with open (log_file, 'rb') as fp :
        line = fp.readline()
        cnt = 1
        while line:
            interpretLine (str (line.strip ()), dico)
            line = fp.readline()
            cnt += 1
    return dico

def dumpLogs (out_file, logs) :
    with open (out_file, 'w') as stream :
        try :
            yaml.dump (logs, stream, default_flow_style=False, allow_unicode=True)
            
        except yaml.YAMLError as exc:
            print(exc)    

def main () :
    parser = argparse.ArgumentParser()

    parser.add_argument("log", help="log file")
    parser.add_argument("-o", "--out_file", help="out file", default="out.yaml")
    args = parser.parse_args ()
    
    log = parseLogFile (args.log)
    dumpLogs (args.out_file, log)

if __name__ == "__main__" :
    main ()
