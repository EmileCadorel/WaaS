package com.orch.daemon.vmware

import org.json4s._
import java.io._
import java.net.ConnectException
import com.orch.utils._
import java.util.{Map => JMap, List => JArray, Properties}
import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConverters._
import akka.actor.{ ActorSelection }
import scala.collection.mutable.Queue
import com.jcraft.jsch._;
import java.util.stream._
import util.control.Breaks._

class MyUserInfo extends UserInfo {
    var password : String = null;

    def getPassword() : String = { println (password); password }
    def setPassword(pass:String) : Unit = { this.password = pass }
     def promptYesNo(str : String) : Boolean = { false }              
     def getPassphrase() : String = { null }
     def promptPassphrase(x: String): Boolean = { false }
     def promptPassword(x: String): Boolean = { false }
     def showMessage(x: String): Unit = {}
}


object VMWare {
    import com.orch.common._
    import com.orch.daemon.kvm._
    import com.orch.daemon.DaemonProto
    import com.orch.utils.{Path, Exe}

    var ESXI = "42.42.2.2"
    var USER = "root";
    var PASS = "tototiti";

    var VM_USER = "phil";
    var VM_PASS = "tototiti";

    var aliasOs = Map ("ubuntu"-> "ubuntu_1804")
    var guestOS = Map ("ubuntu_1804"-> "ubuntu-64")
    var NFS_ADDR = "42.42.1.102"

    var VOLUMES : Map [String, String] = Map ()
    var NET : Array [String] = Array ()
    var STORE = ""

    var STORED_IMAGES : Map [Long, String] = Map ()
    var RUNNING_VMS : Map [Long, String] = Map ();


    /**
      * Start nfs server to give access to it for the VMs
      */
    def startNfsDir (subNet : String = "42.42.1.0/24", nfsAddr : String = "42.42.1.102") : Unit = {
        val export_line = "\"" +  Global.nfs_path + "\" " + subNet + "(rw,no_subtree_check,all_squash,anonuid=0,anongid=0,fsid=180077614)\n"
        val file = new java.io.File (Path ("/etc/exports").file);
        if (file.exists)
            file.delete;

        Path.writeFile (Path ("/etc/exports"), export_line, true)
        val exe = new Exe (Seq ("exportfs", "-ra"), Path ("."))
        exe.synchro ()

        val exe_2 = new Exe (Seq ("service", "nfs-kernel-server", "start"), Path ("."))

        NFS_ADDR = nfsAddr
    }

    def getListOfVolumes () : Unit = {
        val session = createSession (ESXI, USER, PASS)
        val (_, stdout, stderr) = executeSSH (session, "esxcli storage filesystem list |grep '/vmfs/volumes/.*true  VMFS' |sort -nk7")
        for (line <- stdout.split ("\n")) {
            val splitLine = line.split (" ");
            VOLUMES = VOLUMES + (splitLine (0)-> splitLine (2));
            STORE = splitLine (0)
        }
        session.disconnect ()
    }

    def getListOfVMNICs () : Unit = {
        val session = createSession (ESXI, USER, PASS)
        val (_, out, err) = executeSSH (session, "esxcli network vswitch standard list|grep Portgroups|sed 's/^   Portgroups: //g'")
        for (line <- out.split ("\n")) {
            val splitLine = line.split(",|\n")
            NET = NET :+ (splitLine(0))
        }

        session.disconnect ()
    }


    def initVMWareInfos () : Unit = {
        getListOfVolumes ()
        getListOfVMNICs ()
    }

    def createSession (ip : String, user : String, pass : String) : Session = {
        val jsch = new JSch ();
        //jsch.addIdentity(key_path.file)
        val props = new Properties();
        props.put("StrictHostKeyChecking", "no");


        val session=jsch.getSession(user, ip, 22);
        session.setConfig(props);
        session.setPassword (pass);
        session.connect();
        session
    }


    /**
      *  Execute a command inside a VM
      *  Params : 
      *  - ip, the ip of the VM, we access to it with the key pair generated in at the beginning of the daemon
      *  - cmd, the command to launch in the VM
      */
    def executeSSH (session : Session, cmd : String) : (Int, String, String) = {
        val channel=session.openChannel("exec");
        channel.asInstanceOf[ChannelExec].setCommand(cmd);

        val commandOutput = channel.getExtInputStream();

        val outputBuffer = new StringBuilder();
        val errorBuffer = new StringBuilder();

        val in = channel.getInputStream();
        val err = channel.getExtInputStream();

        channel.connect();

        var status = -1
        val len = 1024
        val tmp = Array.ofDim[Byte](len)

        breakable {
            while (true) {
                breakable {
                    while (in.available() > 0) {
                        val i = in.read(tmp, 0, len);
                        if (i < 0) break
                        outputBuffer.append(new String(tmp, 0, i));
                    }
                }

                breakable {
                    while (err.available() > 0) {
                        val i = err.read(tmp, 0, len);
                        if (i < 0) break
                        errorBuffer.append(new String(tmp, 0, i));
                    }
                }

                if (channel.isClosed()) {
                    if ((in.available() <= 0) && (err.available() <= 0)) {
                        status = channel.getExitStatus ()
                        break
                    }
                }

                try {
                    Thread.sleep(200);
                } catch {
                    case e : Exception => {}
                }
            }
        }

        channel.disconnect();        
        return (status, outputBuffer.toString (), errorBuffer.toString ())
    }

    def createVMXFile (id : Long, os : String, cpu : Long, memory : Long) : Array [String] = {
        var vmx : Array [String] = Array ();
        vmx = vmx :+ "config.version = \"8\"";
        vmx = vmx :+ "virtualHW.version= \"13\"";
        vmx = vmx :+ "vmci0.present = \"TRUE\"";
        vmx = vmx :+ "displayName = \"" + s"v_${id}" + "\"";
        vmx = vmx :+ "floppy0.present = \"FALSE\"";
        vmx = vmx :+ "numvcpus = \"" + s"${cpu}" + "\"";
        vmx = vmx :+ "scsi0.present = \"TRUE\""
        vmx = vmx :+ "scsi0.sharedBus = \"none\"";
        vmx = vmx :+ "scsi0.virtualDev = \"pvscsi\"";
        vmx = vmx :+ "memsize = \"" + s"${memory}" + "\"";
        vmx = vmx :+ "scsi0:0.present = \"TRUE\"";
        vmx = vmx :+ "scsi0:0.fileName = \"" + s"v_${id}.vmdk" + "\"";
        vmx = vmx :+ "scsi0:0.deviceType = \"scsi-hardDisk\""
        vmx = vmx :+ "pciBridge0.present = \"TRUE\""
        vmx = vmx :+ "pciBridge4.present = \"TRUE\"";
        vmx = vmx :+ "pciBridge4.virtualDev = \"pcieRootPort\""
        vmx = vmx :+ "pciBridge4.functions = \"8\"";
        vmx = vmx :+ "pciBridge5.present = \"TRUE\"";
        vmx = vmx :+ "pciBridge5.virtualDev = \"pcieRootPort\"";
        vmx = vmx :+ "pciBridge5.functions = \"8\"";
        vmx = vmx :+ "pciBridge6.present = \"TRUE\"";
        vmx = vmx :+ "pciBridge6.virtualDev = \"pcieRootPort\"";
        vmx = vmx :+ "pciBridge6.functions = \"8\"";
        vmx = vmx :+ "pciBridge7.present = \"TRUE\"";
        vmx = vmx :+ "pciBridge7.virtualDev = \"pcieRootPort\"";
        vmx = vmx :+ "pciBridge7.functions = \"8\"";
        vmx = vmx :+ "guestOS = \"" + s"${guestOS (os)}" + "\"";
        vmx = vmx :+ "ethernet0.virtualDev = \"vmxnet3\"";
        vmx = vmx :+ "ethernet0.present = \"TRUE\"";
        vmx = vmx :+ "ethernet0.networkName = \"" + NET (0) + "\"";
        vmx = vmx :+ "ethernet0.addressType = \"generated\"";
        return vmx;
    }

    /**
      * Create the image of the VM, but does not launch it
      * Params : 
      * - id, the id of the VM
      * - os, the os of the VM
      */
    def createVM (id : Long, os : String, cpu : Long, memory : Long) : Unit = {
        val session = createSession (ESXI, USER, PASS);
        val vmx = createVMXFile (id, os, cpu, memory).mkString ("\n");
        var v_path = Path.buildStr (Seq (STORE, s"v_${id}")).file;
        var os_path = Path.buildStr (Seq (STORE, os)).file;
        executeSSH (session, s"mkdir -p ${v_path}");
        executeSSH (session, s"rm ${v_path}/v_${id}.vmx");        
        executeSSH (session, s"echo -e '${vmx}' >> ${v_path}/v_${id}.vmx")

        executeSSH (session, s"vmkfstools -i ${os_path}/${os}.vmdk ${v_path}/v_${id}.vmdk");
        val (_, out, err) = executeSSH (session, s"vim-cmd solo/registervm ${v_path}/v_${id}.vmx");

        this.STORED_IMAGES.synchronized {
            STORED_IMAGES = STORED_IMAGES + (id-> out)
        }

        session.disconnect ()
    }

    def startVM (id : Long) : Unit = {
        val session = createSession (ESXI, USER, PASS)

        val vmId = this.STORED_IMAGES.synchronized {
            STORED_IMAGES (id)
        }

        val (_, out, err) = executeSSH (session, s"vim-cmd vmsvc/power.on ${vmId}")

        // Now we wait for the ip address
        var ip = "";
        while (ip == "") {
            val (_, out, err) = executeSSH (session, s"vim-cmd vmsvc/get.guest ${vmId}")
            breakable {
                for (line <- out.split ("\n")) {
                    if (line.contains ("ipAddress")) {
                        val splitLine = line.split (" = ");
                        if (splitLine (1) != "<unset>,") {
                            val i = splitLine (1).indexOf ("\"")
                            val j = splitLine (1).lastIndexOf ("\"")
                            ip = splitLine (1).slice (i + 1, j)
                            break;
                        }
                    }
                }
            }
            Thread.sleep (200)
        }

        this.RUNNING_VMS.synchronized {
            RUNNING_VMS = RUNNING_VMS + (id -> ip)
        }

        session.disconnect ()
    }

    def mountNfs (id : Long, user : String, ip : String) : Unit = {
        val path_user_vm = Path.buildStr (Seq ("/home/phil/", user));
        Path.mkdirs (Path.buildStr (Seq (Global.nfs_path, user)));

        val path = Path.buildStr (Seq (Global.user_home, user)).file
        val cmd = "mkdir -p " + path_user_vm.file + s"; sudo mount ${ip}:" + path + " " + path_user_vm.file;
        val ipVM = RUNNING_VMS (id)

        while (true) {
            try {
                val session = createSession (ipVM, VM_USER, VM_PASS)
                val (status, out, err) = executeSSH (session, cmd);
                session.disconnect ()
                return
            } catch {
                case e : Throwable => {
                    Thread.sleep (200)
                }
            }
        }
    }

    def registerVM (id : Long, vId : String, ip : String) : Unit = {
        this.RUNNING_VMS.synchronized {
            RUNNING_VMS = RUNNING_VMS + (id-> ip)
        }

        this.STORED_IMAGES.synchronized {
            STORED_IMAGES = STORED_IMAGES + (id-> vId)
        }
    }

    /**
      * Perform all the phase of VM launching, 
      * And send a message to the master once the VM is on and ready
      */
    def launchVM (mid : String, id : Long, os : String, capas : Map [String, Long], user : String, script : String, ref : ActorSelection) : Unit = {
        val thread = new Thread () {
            override def run () : Unit = {
                try {
                    createVM (id, os, capas ("cpus"), capas ("memory"))
                    startVM (id)
                    mountNfs (id, user, NFS_ADDR)
                    ref ! DaemonProto.VmReady (mid, id, "")
                } catch {
                    case _ : InterruptedException => {}
                }
            }
        }

        thread.start ()
    }

    def removeVM (mid : String, id : Long, ref : ActorSelection) : Unit = {
        val thread = new Thread () {
            override def run () {
                killVM (id)
                destroyVM (id)
                ref ! DaemonProto.VmOff (mid, id, "")
            }
        }

        thread.start ()
    }

    /**
      * 
      */
    def killVM (id : Long) : Unit = {
        val session = createSession (ESXI, USER, PASS)

        val vmId = this.STORED_IMAGES.synchronized {
            STORED_IMAGES (id)
        }

        val (_, out, err) = executeSSH (session, s"vim-cmd vmsvc/power.off ${vmId}")

        this.RUNNING_VMS.synchronized {
            RUNNING_VMS = RUNNING_VMS.- (id)
        }

        session.disconnect ();
    }

    /**
      * Destroy the image of a VM
      * Params : 
      * - id, the id of the VM
      */
    def destroyVM (id : Long) : Unit = {
        val vmId = this.STORED_IMAGES.synchronized {
            STORED_IMAGES (id)
        }

        val session = createSession (ESXI, USER, PASS);
        val (_, out, err) = executeSSH (session, s"vim-cmd vmsvc/destroy ${vmId}")
        session.disconnect ()

        this.STORED_IMAGES.synchronized {
            STORED_IMAGES = STORED_IMAGES.- (id)
        }
    }

    /**
      * Run a task in the VM
      */
    def runTask (mid : String, wid : String, id : Long, user : String, v_id : Long, params : String, ref : ActorSelection) : Unit = {
        val ip = this.RUNNING_VMS.synchronized {
            RUNNING_VMS (v_id)
        }

        val e_path = Path.buildStr (Seq (Global.user_home, user, wid, ""+id))
        val path_in_vm = Path.buildStr (Seq ("/home/phil", user, wid, ""+id))
        val innerCommand = "cd " + path_in_vm.file + " && ./launch.sh " + params;

        val thread = new Thread () {
            override def run () : Unit = {
                val untar = new Exe (Seq ("tar", "xf", "exec.tar"), e_path);
                untar.synchro ();
                
                while (true) {
                    try {
                        val session = createSession (ip, VM_USER, VM_PASS)
                        val (status, out, err) = executeSSH (session, innerCommand)
                        if (status != 0) {
                            ref ! DaemonProto.TaskFailure (mid, wid, id, out)
                        } else ref ! DaemonProto.TaskEnd (mid, wid, id)
                        return 
                    } catch {
                        case e: Throwable => {
                            Thread.sleep (200)
                        }
                    }
                }                
            }
        }

        thread.start ()
    }

    def killAllVM () : Unit = {
        this.STORED_IMAGES.synchronized {
            for (v <- this.STORED_IMAGES) {
                killVM (v._1)
                destroyVM (v._1)
            }
        }
    }
}
