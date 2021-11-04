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

def main () :

    jobs = get_current_oar_jobs (["nantes"])
    logger.info ("Current job : " + str (jobs))
    running_jobs = [ job for job in jobs if get_oar_job_info (*job).get ("state") == "Running" ]
    nodes = sorted ([job_nodes for job in running_jobs for job_nodes in get_oar_job_nodes (*job)], key=lambda x: x.address)
    nodes = filter (lambda x: x.address.split (".")[0].split ("-")[0] == "ecotype", nodes)
    
    conn_params = {'user': 'root'}
    for i in range (0,1):
        cmd_str = "java -cp application.jar com.orch.client.Main --addr 172.16.193.13 --port 9000 --eaddr 172.16.193.13 --eport 12000 --path ./montage_617.tgz --user " + str(i) + "_ --deadline 1000"
        print (cmd_str)
        cmd = Remote (cmd_str, nodes [0], conn_params)
        cmd.start ()
        cmd.wait ()

if __name__ == "__main__" :
    main ()
