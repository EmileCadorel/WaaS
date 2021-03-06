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

## Known information about nantes clusters
known_roots = {"ecotype" : "172.16.193.", "econome" : "172.16.192."}
known_caps = {"ecotype" : {"cpus" : 20, "memory" : 32768}, "econome" : {"cpus" : 16, "memory" : 16000}}
known_speed = {"ecotype" : 1800, "econome" : 2200}

# Return the addr (ip address), and cluster name of the node
# -> (addr, cluster)
def getAddrOfNode (node) :
    cluster = ""
    addr = node.address
    cluster = addr.split (".")[0].split ("-")[0]
    res = known_roots [cluster] + (addr.split (".")[0].split ("-")[1])
    return (res, cluster)

def getNodes (started) : 
    jobs = get_current_oar_jobs (["nantes"])
    logger.info ("Current job : " + str (jobs))

    running_jobs = [ job for job in jobs if get_oar_job_info (*job).get ("state") == "Running" ]
    nodes = sorted ([job_nodes for job in running_jobs for job_nodes in get_oar_job_nodes (*job)], key=lambda x: x.address)
    logger.info ("Will deploy on : " + str (nodes))
    
    deployed, undeployed = deploy (Deployment (nodes, env_name="ubuntu_1804"), check_deployed_command=started)
    print ("Deployed")
    return nodes

# Deploy image on grid5000 nodes, that can run a Docker hypervisor
# This image is deployed with kadeploy
def deployDocker (started, nodes) :
    conn_params = {'user': 'root'}
    if (started == False) : 
        update = Remote (
            "sudo apt update",
            nodes, conn_params
        )

        logger.info ("Deployed, running apt update")
    
        update.start ()
        update.wait ()

        install_deps = Remote (
            "sudo apt install -y libvirt-bin openjdk-11-jre-headless openjdk-11-jdk-headless ruby curl libguestfs-tools dnsmasq-utils iperf docker.io",
            nodes, conn_params
        )

        logger.info ("Installing dependencies")
    
        install_deps.start ()
        install_deps.wait ()
    
    return nodes 

# Deploy image on grid5000 nodes, that can run a KVM hypervisor
# This image is deployed with kadeploy
def deployKVM (started, nodes) :    
    conn_params = {'user': 'root'}
    if (started == False) : 
        update = Remote (
            "sudo apt update",
            nodes, conn_params
        )

        logger.info ("Deployed, running apt update")
    
        update.start ()
        update.wait ()

        install_deps = Remote (
            "sudo apt install -y libvirt-bin openjdk-11-jre-headless openjdk-11-jdk-headless ruby curl libguestfs-tools dnsmasq-utils iperf",
            nodes, conn_params
        )

        logger.info ("Installing dependencies")
    
        install_deps.start ()
        install_deps.wait ()
    
    return nodes 


# Send the image to boot VM on the nodes 
def sendImagesDocker (nodes) :
    logger.info ("Send images on node Docker : " + str (nodes))
    conn_params = {'user': 'root'}
    send = Put (nodes, ["Dockerfile"], ".", connection_params=conn_params)

    send.run ()
    cmd = "docker build . -t ubuntu"
    run_docker = Remote (cmd, nodes, conn_params)    
    send.wait ()
    run_docker.run ()
    run_docker.wait ()


# Send the image to boot VM on the nodes 
def sendImagesKVM (nodes) :
    logger.info ("Send images on node KVM : " + str (nodes))
    conn_params = {'user': 'root'}
    send = Put (nodes, ["images"], ".", connection_params=conn_params)

    send.run ()
    send.wait ()
    
# Send the java application on the nodes
def sendJar (nodes) :
    logger.info ("Send jars on node : " + str (nodes))
    conn_params = {'user': 'root'}
    send = Put (nodes, ["application.jar"], ".", connection_params=conn_params)

    send.run ()
    send.wait ()
    
# Create an config file for a daemon and send it to it
def createConfigForDaemonDocker (node) :
    logger.info ("Create config for daemon : " + str (node))
    cluster = node.address.split (".")[0].split("-")[0]
    content = {
        "name" : node.address,
        "speed" : known_speed [cluster],
        "capacities" : known_caps [cluster],
        "boot_times" : {"ubuntu" : 1}
    }
    
    with open ("config-" + node.address + ".yaml", "w") as stream :
        try :
            yaml.dump (content, stream, default_flow_style=True, allow_unicode=True)
        except yaml.YAMLError as exc : 
            print (exc)
            
    conn_params = {'user': 'root'}
    send = Put (node, ["config-" + node.address + ".yaml"], connection_params=conn_params)
    send.run ()
    os.remove ("config-" + node.address + ".yaml")
    return known_roots [cluster] + (node.address.split (".")[0].split ("-")[1])

# Create an config file for a daemon and send it to it
def createConfigForDaemonKVM (node) :
    logger.info ("Create config for daemon : " + str (node))
    cluster = node.address.split (".")[0].split("-")[0]
    content = {
        "name" : node.address,
        "speed" : known_speed [cluster],
        "capacities" : known_caps [cluster],
        "boot_times" : {"ubuntu" : 30}
    }
    
    with open ("config-" + node.address + ".yaml", "w") as stream :
        try :
            yaml.dump (content, stream, default_flow_style=True, allow_unicode=True)
        except yaml.YAMLError as exc : 
            print (exc)
            
    conn_params = {'user': 'root'}
    send = Put (node, ["config-" + node.address + ".yaml"], connection_params=conn_params)
    send.run ()
    os.remove ("config-" + node.address + ".yaml")
    return known_roots [cluster] + (node.address.split (".")[0].split ("-")[1])


# Create a cluster composed of a master and a list of daemons
# The entry, is the master node of an another cluster that will make this
# cluster join the global topology
def createClusterDocker (master, daemons, entry) : 
    # Here the jar is already on the node
    # All the VM images are also on the daemons
    # We just need to run it
    
    conn_params = {'user': 'root'}
    (masterAddr, masterCluster) = getAddrOfNode (master)
    masterCmd = "java -cp application.jar com.orch.master.Main --addr " + masterAddr + " --port 5000 --sched HEFT_DEADLINE"
    (entryAddr, entryCluster) = getAddrOfNode (entry)
    masterCmd = masterCmd + " --eaddr " + entryAddr + " --eport 12000"
    
    masterCmd = masterCmd + " > master.out.txt 2> master.error.txt" # Dumping the logs of the master
    
    run_master = Remote (masterCmd, master, conn_params)
    logger.info ("Launching Master on node : " + str (master))
    run_master.start ()
    
    sleep (1) # We sleep a second, to make sure the master is well launched
    
    # Now launching the daemon on each node
    for d in daemons :
        (dAddr, c) = getAddrOfNode (d)
        daemonCmd = "java -cp application.jar com.orch.daemon.Main --addr " + dAddr + " --port 4000 --maddr " + masterAddr + " --mport 5000 --config config-" + d.address + ".yaml --type Docker > daemon.out.txt 2> daemon.error.txt"
        # The config file is located on the daemon already
        
        logger.info ("Launching Daemon on node : " + str (d) + " with master : " + str (master))
        run_daemon = Remote (daemonCmd, d, conn_params)
        run_daemon.start ()
    return run_master


# Create a cluster composed of a master and a list of daemons
# The entry, is the master node of an another cluster that will make this
# cluster join the global topology
def createClusterKVM (master, daemons, entry) : 
    # Here the jar is already on the node
    # All the VM images are also on the daemons
    # We just need to run it
    
    conn_params = {'user': 'root'}
    (masterAddr, masterCluster) = getAddrOfNode (master)
    masterCmd = "java -cp application.jar com.orch.master.Main --addr " + masterAddr + " --port 5000 --sched HEFT_DEADLINE"
    (entryAddr, entryCluster) = getAddrOfNode (entry)
    masterCmd = masterCmd + " --eaddr " + entryAddr + " --eport 12000"
    
    masterCmd = masterCmd + " > master.out.txt 2> master.error.txt" # Dumping the logs of the master
    
    run_master = Remote (masterCmd, master, conn_params)
    logger.info ("Launching Master on node : " + str (master))
    run_master.start ()
    
    sleep (5) # We sleep a second, to make sure the master is well launched
    
    # Now launching the daemon on each node
    for d in daemons :
        (dAddr, c) = getAddrOfNode (d)
        daemonCmd = "java -cp application.jar com.orch.daemon.Main --addr " + dAddr + " --port 4000 --maddr " + masterAddr + " --mport 5000 --config config-" + d.address + ".yaml --type KVM > daemon.out.txt 2> daemon.error.txt"
        # The config file is located on the daemon already
        
        logger.info ("Launching Daemon on node : " + str (d) + " with master : " + str (master))
        run_daemon = Remote (daemonCmd, d, conn_params)
        run_daemon.start ()
    return run_master

def createLeader (leader) :
    conn_params = {'user': 'root'}
    (masterAddr, masterCluster) = getAddrOfNode (leader)
    
    masterCmd = "java -cp application.jar com.orch.leader.Main --addr " + masterAddr + " --port 12000 --sched HEFT_DEADLINE"
    masterCmd = masterCmd + " > leader.out.txt 2> leader.error.txt" # Dumping the logs of the master
    run_master = Remote (masterCmd, leader, conn_params)
    logger.info ("Launching Leader on node : " + str (leader))
    run_master.start ()
    return run_master

nodes = sorted (getNodes (False), key=lambda x: x.address)
addr = {}
logger.info ("Nodes deployed")

econome = filter (lambda x: x.address.split (".")[0].split ("-")[0] == "econome", nodes)
ecotype = filter (lambda x: x.address.split (".")[0].split ("-")[0] == "ecotype", nodes)[0:44]

ecotype_1 = ecotype [0:11]
ecotype_2 = ecotype [11:22]
ecotype_3 = ecotype [22:33]
ecotype_4 = ecotype [33:44]

deployDocker (False, ecotype_1)
deployDocker (False, ecotype_2)
deployDocker (False, econome)
deployKVM    (False, ecotype_3)
deployKVM    (False, ecotype_4)

#if not True : 
sendImagesDocker (econome)
sendImagesDocker (ecotype_1)
sendImagesDocker (ecotype_2)
sendImagesKVM (ecotype_3)
sendImagesKVM (ecotype_4)

sendJar (nodes)

for n in econome :
    addr [n.address] = createConfigForDaemonDocker (n)

for n in ecotype_1 :
    addr [n.address] = createConfigForDaemonDocker (n)

for n in ecotype_2 :
    addr [n.address] = createConfigForDaemonDocker (n)
    
for n in ecotype_3 :
    addr [n.address] = createConfigForDaemonKVM (n)

for n in ecotype_4 :
    addr [n.address] = createConfigForDaemonKVM (n)
    
print (addr)

run_master = []
run_master = [createLeader (ecotype_1 [0])]
run_master = run_master + [createClusterDocker (econome [0], econome, ecotype_1[0])]
run_master = run_master + [createClusterDocker (ecotype_1[0], ecotype_1, ecotype_1[0])]
run_master = run_master + [createClusterDocker (ecotype_2[0], ecotype_2, ecotype_1[0])]
run_master = run_master + [createClusterKVM (ecotype_3[0], ecotype_3, ecotype_1[0])]
run_master = run_master + [createClusterKVM (ecotype_4[0], ecotype_4, ecotype_1[0])]

for n in run_master : 
    n.wait ()
