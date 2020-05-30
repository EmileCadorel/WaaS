package com.orch.daemon.kvm

import org.json4s._
import java.io._
import java.net.ConnectException
import com.orch.utils._
import java.util.{Map => JMap, List => JArray}
import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConverters._
import akka.actor.{ ActorSelection }
import scala.collection.mutable.Queue
import com.jcraft.jsch._;
import java.util.stream._
import util.control.Breaks._
import com.orch.daemon.kvm.Global._
import com.orch.daemon._

class MyUserInfo extends UserInfo {
     def getPassword() : String = { null }
     def promptYesNo(str : String) : Boolean = { true }              
     def getPassphrase() : String = { null }
     def promptPassphrase(x: String): Boolean = { true }
     def promptPassword(x: String): Boolean = { true }
     def showMessage(x: String): Unit = {}
}

/**
  * A bunch of stuff that allows to manage VMs with KVM hypervisor
  */
object KVM {
    import com.orch.common._
    import com.orch.daemon.kvm._
    import com.orch.daemon.DaemonProto._
    import com.orch.utils.{Path, Exe}

    var mac_of_vms : Map [Long, String] = Map ()
    var ips_of_vms : Map [Long, String] = Map ()
    var booting_vms : Map [Long, Thread] = Map ()
    val boot_conflict : Integer = new Integer (0)

    /**
      * Generate private, and public keys
      * Those keys will be used to access to the VMs
      */
    def initKeySecurity () : Unit = {
        val keyFile = Path.build (Global.key_path, "key").file
        val file = new java.io.File (keyFile);
        if (file.exists)
            file.delete;
        Path.mkdirs (Path (Global.key_path));

        val exe = new Exe (Seq ("ssh-keygen", "-b", "2048", "-t", "rsa", "-f", keyFile, "-q", "-N", s""), Path ("."));
        exe.synchro ();
    }

    /**
      * Start nfs server to give access to it for the VMs
      */
    def startNfsDir () : Unit = {
        val export_line = "\"" +  Global.nfs_path + "\" 192.168.122.0/24(rw,no_subtree_check,all_squash,anonuid=0,anongid=0,fsid=180077614)\n"
        val file = new java.io.File (Path ("/etc/exports").file);
        if (file.exists)
            file.delete;

        Path.writeFile (Path ("/etc/exports"), export_line, true)
        val exe = new Exe (Seq ("exportfs", "-ra"), Path ("."))
        exe.synchro ()

        val exe_2 = new Exe (Seq ("service", "nfs-kernel-server", "start"), Path ("."))
        println (exe_2.synchro ())
    }

    /**
      * Create file for user data, when creating a new VM
      * Params : 
      * - user, the user who possess the VM
      * - ssh_key, the path to the public key file
      */
    def user_data (user : String, ssh_key : Path) : String = {
        val key = Path.readFile (ssh_key)

        var data : Map [String, Object] = Map ()
        data = data + ("groups" -> "wheel");
        data = data + ("lock_passwd" -> "false");
        data = data + ("name"->  "phil");
        data = data + ("shell" ->  "/bin/bash");
        data = data + ("ssh-authorized-keys" -> List (key).asJava);
        data = data + ("sudo" -> List ("ALL=(ALL) NOPASSWD:ALL").asJava);

        var users : Map [String, Object] = Map ()
        users = users + ("users" -> List (data.asJava).asJava)

        var yaml = new Yaml();
        var writer = new StringWriter();
        yaml.dump(users.asJava, writer);

        "#cloud-config\n---\n" + writer.toString ()
    }

    /**
      *  Create the meta data file for a VM
      *  Params : 
      *  - id, the id of the VM
      */
    def meta_data (id : String) : String = {
        var data : Map [String, Object] = Map ()
        data = data + ("instance-id" -> id)
        data = data + ("local-hostname" -> id)

        var yaml = new Yaml();
        var writer = new StringWriter();
        yaml.dump(data.asJava, writer);

        writer.toString ()
    }

    /**
      * Create the image of a VM, but does not lauch it
      * Params :
      * - id, the id of the VM
      * - os, the OS of the VM that will be boot
      */
    def createDirAndFileOfVM (id : Long, os : String) : Unit = {
        val os_qcow2 = Path.build (Global.os_path, os + ".qcow2")
        val v_path = Path.build (Global.vm_path, s"v$id")
        val os_dst = Path.build (Path.build (Global.vm_path, s"v$id"), s"v$id.qcow2")
        println ("VM path : " + Global.vm_path + " ")
        Path.mkdirs (v_path)
        Path.copyFile (os_qcow2, os_dst)
        Path.setPermission (os_dst)

        Path.writeFile (Path.build (v_path, "user-data"), user_data ("phil", Path.build (Global.key_path, "key.pub")))
        Path.writeFile (Path.build (v_path, "meta-data"), meta_data (s"v$id"))

        val exe = new Exe (
            Seq ("mkisofs", "-o",
                Path.build (v_path, "user.iso").file,
                "-V", "cidata", "-J", "-r",
                Path.build (v_path, "user-data").file,
                Path.build (v_path, "meta-data").file),
            v_path)

        exe.synchro ()

        val prep = new Exe (
            Seq ("virt-sysprep", "-a", os_dst.file), v_path
        )

        prep.synchro ()
    }    

    /**
      *  Create a new VM and boot it
      *  Params : 
      *  - id, the id of the VM
      *  - capas, the capacities of the VM
      */
    def installAndLaunchVM (id : Long, capas : Map [String, Long]) : Unit = {
        val v_path = Path.build (Global.vm_path, s"v$id")
        val os_dst = Path.build (v_path, s"v$id.qcow2")
        val iso_dst = Path.build (v_path, "user.iso")
        val exe = new Exe (Seq ("virt-install",
            "--import",
            "--name", s"v$id",
            "--ram", "" + capas ("memory"),
            "--vcpus", "" + capas ("cpus"),
            "--disk", os_dst.file + ",format=qcow2,bus=virtio",
            "--disk", iso_dst.file + ",device=cdrom",
            "--network", "bridge=virbr0,model=virtio",
            "--os-type", "linux",
            "--os-variant", "ubuntu18.04",
            "--virt-type", "kvm",
            "--noautoconsole"
        ), Path ("."))

        this.synchronized { // Visiblement on peut pas trop faire ça en parallèle
            var good = false
            while (!good) {
                val (res_, out_, err_) = exe.synchro ()
                if (res_ == 0) good = true
                println (out_, " ", err_)
            }
        }
    }

    /**
      * Wait for a VM to be available
      * And store its ip and mac address
      * Params : 
      * - id, the id of the VM
      */
    def waitAndStoreIp (id : Long) : String = {
        import org.json4s.native.JsonMethods._

        var getMac = false
        var mac_addr = ""
        while (!getMac) {
            val exe = new Exe (Seq ("virsh", "dumpxml", s"v$id"), Path ("."))
            val (res, out, err) = exe.synchro ()
            val doc = scala.xml.XML.loadString (out)
            val mac = doc \\ "mac"
            if (mac.length != 0) {
                getMac = true
                mac_addr = mac (0).attribute ("address").getOrElse ("").toString ()
            }
        }

        this.synchronized {
            mac_of_vms = mac_of_vms + (id -> mac_addr)
        }

        while (true) {
            val json_file = parse (Path.readFile (Path ("/var/lib/libvirt/dnsmasq/virbr0.status")))
            if (json_file.values != None) {
                val values = json_file.values.asInstanceOf[List[Map[String, String]]]
                for (vm_node <- values) {
                    if (vm_node ("mac-address") == mac_addr) {
                        this.synchronized { // putain de scala à la con
                            ips_of_vms = ips_of_vms + (id -> vm_node ("ip-address"))
                        }
                        println ("IP : " + vm_node ("ip-address") + " " + id + " " + ips_of_vms)
                        return vm_node ("ip-address")
                    }
                }
            }
            Thread.sleep (200)
        }
        ""
    }

    /**
      *  Execute a command inside a VM
      *  Params : 
      *  - ip, the ip of the VM, we access to it with the key pair generated in at the beginning of the daemon
      *  - cmd, the command to launch in the VM
      */
    def executeInVM (ip : String, cmd : String) : (Int, String, String) = {
        println (s"CMD ssh : $ip : $cmd")
        val key_path = Path.build (Global.key_path, "key")

        val jsch = new JSch ();
        jsch.addIdentity(key_path.file)
        val session=jsch.getSession("phil", ip, 22);
        val ui=new com.orch.daemon.kvm.MyUserInfo();
        session.setUserInfo(ui);
        session.connect();
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
                        System.out.println("exit-status: " + status);
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

        System.out.println("output: " + outputBuffer.toString());
        System.out.println("error: " + errorBuffer.toString());

        channel.disconnect();        
        session.disconnect();        
        return (status, outputBuffer.toString (), errorBuffer.toString ())
    }

    /**
      *  Mount a shared folder in a VM
      */
    def mountNfsSharedFolder (id : Long, ip : String, user : String) : Unit = {
        val path_user_vm = Path.build ("/home/phil/", user)        
        Path.mkdirs (Path.build (Global.nfs_path, user))

        val path = Path.buildStr (Seq (Global.user_home, user)).file
        val cmd = "mkdir " + path_user_vm.file + "; sudo mount 192.168.122.1:" + path + " " + path_user_vm.file;

        while (true) {
            try {
                val (status, out, err) = executeInVM (ip, cmd)
                return
            } catch {
                case e : Throwable => {
                    println (e)
                    Thread.sleep (200)
                }
            }
        }
    }

    /**
      *  Launch a new VM
      *  This will call the master when the VM is ready
      */
    def launchVM (mid : String, id : Long, os : String, capas : Map [String, Long], user : String, script : String, ref : ActorSelection) : Unit = {
        val thread = new Thread () {
            override def run () : Unit = {
                try {
                    createDirAndFileOfVM (id, os)
                    installAndLaunchVM (id, capas)
                    val ip = waitAndStoreIp (id)
                    mountNfsSharedFolder (id, ip, user)
                    ref ! DaemonProto.VmReady (mid, id, "")
                } catch {
                    case _ : InterruptedException => {}
                }
            }
        }

        this.synchronized {
            booting_vms = booting_vms + (id -> thread)            
        }

        thread.start ()
    }

    /**
      *  Remove a vm from the list of currently booting VM
      */
    def removeBooting (id : Long) : Unit = {
        this.synchronized {
            this.booting_vms = this.booting_vms.filterKeys (_ != id)
        }
    }    

    def getListOfFiles(dir: String):List[java.io.File] = {
        val d = new java.io.File(dir)
        if (d.exists && d.isDirectory) {
            d.listFiles.toList
        } else {
            List[java.io.File]()
        }
    }

    /**
      *  Run a task in a VM
      *  Params : 
      *  - mid, the id of the node (used only for the protocol of communication with the master)
      *  - id, the id of the task
      *  - user, the user to whom the task belong
      *  - v_id, the id of the VM in which we want the task to be run
      *  - params, the parameters to pass to the task
      *  - ref, the actor selection, reference of the Master module
      * This will inform the master module, in case of success or failure
      */
    def runTask (mid : String, wid : String, id : Long, user : String, v_id : Long, params : String, ref : ActorSelection) : Unit = {
        val ip = this.synchronized {
            if (this.ips_of_vms.contains (v_id))
                this.ips_of_vms (v_id)
            else {
                println (s"No vm : $v_id ${ips_of_vms}")
                ref ! DaemonProto.TaskFailure (mid, wid, id, "")
                return 
            }
        }

        val e_path = Path.buildStr (Seq (Global.user_home, user, wid, ""+ id))
        val path_in_vm = Path.buildStr (Seq ("/home/phil", user, wid, "" + id))

        val innerCommand = "cd " + path_in_vm.file + " && ./launch.sh " + params;

        val thread = new Thread () {
            override def run () : Unit = {
                val untar = new Exe (Seq ("tar", "xf", "exec.tar"), e_path)
                untar.synchro ()
                val list = getListOfFiles (e_path.file);

                while (true) {
                    try {

                        val (status, out, err) = executeInVM (ip, innerCommand)
                        println (id + " " + status + " " + out + " " + err)
                        for (f <- list) { // remove all input and exec
                            deleteRecursively (f);
                        }

                        if (status != 0) {
                            ref ! DaemonProto.TaskFailure (mid, wid, id, out)
                        } else ref ! DaemonProto.TaskEnd (mid, wid, id)
                        return
                    } catch {
                        case e: Throwable => {
                            println (e)
                            Thread.sleep (200)
                        }
                    }
                }
            }
        }



        thread.start ()
    }

    /**
      *  Kill a VM, booting, running of already killed, that's not important
      *  Params : 
      *  - mid, the id of the node (just for the master module)
      *  - id, the id of the VM to be killed
      *  - ref, the actor selection, reference of the master module
      */
    def killVM (mid : String, id : Long, ref : ActorSelection) {
        println ("KILL : " + id);
        val thread = new Thread () {
            override def run () {
                this.synchronized {
                    if (booting_vms.contains (id)) {
                        println (s"INTERRUPT : ${id}")
                        booting_vms (id).interrupt ()
                        removeBooting (id)
                    }
                }

                val destroy = new Exe (Seq ("virsh", "destroy", s"v$id"), Path ("."))
                val undefine = new Exe (Seq ("virsh", "undefine", s"v$id"), Path ("."))

                destroy.synchro ()
                undefine.synchro ()

                ref ! DaemonProto.VmOff (mid, id, "")

                var mac = ""

                this.synchronized {
                    if (mac_of_vms.contains (id))
                        mac = mac_of_vms (id)
                    mac_of_vms = mac_of_vms.filterKeys (_ != id)
                }

                deleteRecursively (new java.io.File (Path.build (Global.vm_path, s"v$id").file))

                var ip = ""                
                this.synchronized {
                    if (ips_of_vms.contains (id))
                        ip = ips_of_vms (id)
                }

                if (ip != "" && mac != "") {
                    val lease = new Exe (Seq ("dhcp_release", "virbr0", ip, mac), Path ("."))
                    lease.synchro ()
                }
            }
        }

        thread.run ()
    }

    def killAllVM () : Unit = {
        for (v <- ips_of_vms) {
            val destroy = new Exe (Seq ("virsh", "destroy", s"v${v._1}"), Path ("."))
            val undefine = new Exe (Seq ("virsh", "undefine", s"v${v._2}"), Path ("."))
            destroy.synchro ()
            undefine.synchro ()

            deleteRecursively (new java.io.File (Path.build (Global.vm_path, s"v${v._1}").file))
        }
    }

    def deleteRecursively(file: java.io.File): Unit = {
        if (file.isDirectory)
            file.listFiles.foreach(deleteRecursively)

        if (file.exists)
            file.delete
    }

}
