# WaaS

This repository contains the source code of the WaaS cloud service,
deployment script to launch the service on
(Grid5000)[https://www.grid5000.fr/w/Grid5000:Home], and some traces
acquired during experimentations.

# Compile 

The folder *source* contains the source code in scala. Run the command the following command, to get a jar executable file.

```
sbt assembly
```

The jar file contains multiple main for each module of the WaaS.
- com.orch.master.Main, the master module
- com.orch.daemon.Main, the worker module
- com.orch.leader.Main, the leader module
- com.orch.client.Main, the client module

# Launching

To start we need to launch a leader module, on a node.

```
java -cp waas.jar com.orch.leader.Main --addr 172.16.193.1 --port 12000 --sched HEFT
```

Then launch each master module, one of them must be on the same node as the leader module (limitation of this implementation, they must share the same database). To each master module, specify the address and port of the leader (--eaddr, --eport).

```
java -cp waas.jar com.orch.master.Main --addr 172.16.193.1 --port 5000 --eaddr 172.16.193.1 --eport 12000
```

Then launch the workers, the workers have entry point which is the master module responsible of their cluster. A config file is required. 

```
java -cp waas.jar com.orch.daemon.Main --addr 172.16.193.1 --port 4000 --eaddr 172.16.193.1 --eport 5000 --config config.yaml --type Docker
```

Example of configuration file : 
```yaml
name : ecotype-1
speed : 1800
capacities :
    cpus : 20
    memory : 128000
boot_times :
    ubuntu : 1
```

The service is now ready to receive client request. The client can be launch anywhere as long as the leader module can access its ip address.

```
java -cp waas.jar com.orch.client.Main --addr 172.16.193.2 --port 9000 --eaddr 172.16.193.1 --eport 12000 --path application.tgz --user alice [--deadline 1000]
````

# Example 

An example of launching is presented for the (Grid5000)[https://www.grid5000.fr/w/Grid5000:Home] infrastructure in *G5K/deploy-clusters.py*. This example start 5 virtual clusters on the 2 clusters *econome* and *ecotype* located in Nantes. Two of them are running KVM workers.

The KVM workers needs an image of ubuntu to start correclty, but the vm image is not provided on this repository. The docker workers are fully configured by the script.

# Results 

The directory *results* contains traces of execution acquired by experimentations. There are four algorithm tested in the experiments.
- HEFT
- HEFT-deadline
- Min-Min
- Max-Min

Scripts to analyse the traces are present. 
- parser.py, read a trace file and generate a report in yaml
- times.py, read a yaml report and print some results


Multiple traces can be assembled to by analysed by the parser.

```
cat ecotype-1.txt ecotype-2.txt > ecotype-1-2.txt
python parser.py ecotype-1-2.txt -o ecotype-1-2.report.yaml
python times.py ecotype-1-2.report.yaml
```

Example of report : 
```yaml
daemons : # list of workers
- ecotype-1.nantes.grid5000.fr
- ecotype-2.nantes.grid5000.fr
tasks : : # list of executed tasks
  1-master@172.16.193.1_5000/1:
    duration: 0:00:23
    end: 05/27/2020 19:11:14
    name: mProject
    start: 05/27/2020 19:10:51
    workflow: 1-master@172.16.193.13_5000
  ...
vms: # list of provisionned virtuals resources (including container)
  '1': 
    boot: 0:00:01
    duration: 0:01:54
    end: 05/27/2020 19:12:43
    node: ecotype-14.nantes.grid5000.fr
    ready: 05/27/2020 19:10:50
    start: 05/27/2020 19:10:49
  ...
workflows: # list of executed workflows
  1-master@172.16.193.13_5000:
    duration: 0:01:58
    end: 05/27/2020 19:12:42
    start: 05/27/2020 19:10:44
  ...
```
