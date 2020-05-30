package com.orch.leader.configuration

class Configuration () {

    /**
      * The list of vms, sorted by their start time
      */
    var vmsByStart : Map [Long, Array [Long]] = Map ()

    /**
      * The list of vms, indexed by their ids
      */
    var vmsById : Map [Long, CVM] = Map ()

    /**
      * The list of tasks, sorted by their start time
      * The tasks in that list are ready to launch, 
      *  they have both ready files, and started VM
      */
    var tasksByStart : Map [Long, Array [String]] = Map ()

    /**
      * The list of all tasks, indexed by their id
      */
    var tasksById : Map [String, CTask] = Map ()

    /**
      * The association between tasks and VMs
      */
    var tasksByVM : Map [Long, Array [String]] = Map ()

    /**
      * The list of all files, indexed by their id
      */
    var filesById : Map [String, Array [CFile]] = Map ()

    /**
      * The list of input file of a task, ._1 = task id
      */
    var fileByEntry : Map [String, Array [CFile]] = Map ()


    var tasksFinished : Map [String, CTask] = Map ()

    /**
      * Insert a new VM in the configuration, or replace it if it did already exist
      * The VM will be accessible, by vmById, and vmsByTime
      */
    def insertOrUpdate (vm : CVM) : Unit = {
        if (vmsById.contains (vm.id)) {
            var ancVm = vmsById (vm.id)
            if (ancVm.state == CVMState.OFF) { // If the VM is off, we can change its start time
                var array : Array [Long] = if (vmsByStart.contains (ancVm.start)) {
                    vmsByStart (ancVm.start).filter (_ != ancVm.id)
                } else Array ()

                if (array.length != 0)
                    vmsByStart = vmsByStart + (ancVm.start -> array)
                else vmsByStart = vmsByStart.- (ancVm.start)

                vmsByStart = vmsByStart + (vm.start -> (array :+ vm.id))
            }
        } else {
            vmsById = vmsById + (vm.id -> vm)
            var array : Array [Long] = if (vmsByStart.contains (vm.start)) {
                vmsByStart (vm.start)
            } else Array ()

            vmsByStart = vmsByStart + (vm.start -> (array :+ vm.id))
        }
    }

    /**
      * Insert or update a task in the configuration
      * The task will be accessible by taskByIds
      */
    def insertOrUpdate (task : CTask) : Unit = {
        tasksById = tasksById + ((task.wid + "/" + task.id)-> task)
        val array : Array [String] = if (tasksByVM.contains (task.vid))
            tasksByVM (task.vid)
        else Array ()

        tasksByVM = tasksByVM + (task.vid -> (array :+ (task.wid + "/" + task.id)))
    }

    /**
      * We found the executable for the task whose id is /id/
      * If the VM and input files of the task are ok, it is inserted in taskByStart
      */
    def executableFound (id : String, path : String) : Unit = {
        val task = tasksById (id)
        task.state = CTaskState.HAS_EXEC
        task.app = path
        tasksById = tasksById + (id-> task)
    }

    /**
      * Insert or update a list of files, that are attached to a single file instance
      * All the files will be findable in filesById, and the input ones also in inputs
      */
    def insertOrUpdate (name : String, files : Array [CFile]) : Unit = {
        val array : Array [CFile] = if (filesById.contains (name))
            filesById (name)
        else Array ()

        filesById = filesById + (name-> (files ++ array))

        var inputs : Array [CFile] = Array ()
        for (f <- files) {
            if (f.io == CFileType.INPUT) {
                inputs = inputs :+ f
            }
            if (f.deps != -1) {
                val taskId = f.wid + "/" + f.deps;
                val entries : Array [CFile] = if (fileByEntry.contains (taskId))
                    fileByEntry (taskId)
                else Array ()

                fileByEntry = fileByEntry + (taskId-> (entries :+ f))
            }
        }
    }

    def getTaskById (wid : String, id : Long) : CTask = {
        this.tasksById (wid + "/" + id)
    }

    def getTaskById (tid : String) : CTask = {
        this.tasksById (tid)
    }

    def getVMById (vid : Long) : CVM = {
        this.vmsById (vid)
    }


    def getFilesById (id : String) : Array [CFile] = {
        this.filesById (id)
    }

    def getFilesByEntry (id : String) : Array [CFile] = {
        if (this.fileByEntry.contains (id))
            this.fileByEntry (id)
        else Array ()
    }

    def updateFileEntry (taskId : String, files : Array [CFile]) : Unit = {
        if (files.length == 0) {
            this.fileByEntry = this.fileByEntry.- (taskId)
        } else {
            this.fileByEntry = this.fileByEntry + (taskId-> files)
        }
    }

    def updateFileById (fid : String, files : Array [CFile]) : Unit = {
        if (files.length == 0) {
            this.filesById = this.filesById.- (fid)
        } else {
            this.filesById = this.filesById + (fid-> files)
        }
    }

    def insertLaunchable (task : CTask) : Unit = {
        val array : Array [String] = if (this.tasksByStart.contains (task.start))
            this.tasksByStart (task.start)
        else Array ()

        this.tasksByStart = this.tasksByStart + (task.start -> (array :+ (task.wid+"/"+task.id)))
    }

    /**
      * Find the first thing to do in the list of VM and Task
      */
    def computeMinTime () : Long = {
        val starts = this.vmsByStart.keySet.toList ++ this.tasksByStart.keySet.toList
        if (starts.length > 0) {
            starts.sortWith (_ < _) (0)
        } else
              -1
    }

    def bootingVM (id : Long, start : Long) : Unit = {
        if (this.vmsByStart.contains (start)) {
            var array = this.vmsByStart (start).filter (_ != id)
            if (array.length != 0)
                this.vmsByStart = this.vmsByStart + (start -> array)
            else this.vmsByStart = this.vmsByStart.- (start)
        }

        if (this.vmsById.contains (id)) {
            var vm = this.vmsById (id)
            vm.state = CVMState.BOOTING
            this.vmsById = this.vmsById + (id-> vm)
        }
    }

    def runningVM (id : Long) : Unit = {
        if (this.vmsById.contains (id)) {
            var vm = this.vmsById (id)
            vm.state = CVMState.RUNNING
            this.vmsById = this.vmsById + (id-> vm)
        }
    }

    def killedVM (id : Long) : Unit = {
        this.vmsById = this.vmsById.- (id)
    }

    def getVMsByStart () : Map [Long, Array [Long]] = {
        this.vmsByStart
    }

    def getTasksByStart () : Map [Long, Array [String]] = {
        this.tasksByStart
    }

    def startTask (id : String, start : Long) : Unit = {
        if (this.tasksByStart.contains (start)) {
            var array = this.tasksByStart (start).filter (_ != id)
            if (array.length != 0)
                this.tasksByStart = this.tasksByStart + (start -> array)
            else this.tasksByStart = this.tasksByStart.- (start)
        }

        if (this.tasksById.contains (id)) {
            var vm = this.tasksById (id)
            vm.state = CTaskState.RUNNING
            this.tasksById = this.tasksById + (id-> vm)
        }
    }

    def endTask (id : String) : Unit = {
        if (this.tasksById.contains (id)) {
            val task = this.tasksById (id)
            this.tasksFinished = this.tasksFinished + (id-> task)

            this.tasksById = this.tasksById. -(id)
            val array = this.tasksByVM (task.vid).filter (_ != id)
            if (array.length != 0)
                this.tasksByVM = this.tasksByVM + (task.vid -> array)
            else this.tasksByVM = this.tasksByVM.- (task.vid)
        }
    }

    def getTasksByVM (vid : Long) : Array [String] = {
        if (this.tasksByVM.contains (vid))
            this.tasksByVM (vid)
        else Array ()
    }

}

