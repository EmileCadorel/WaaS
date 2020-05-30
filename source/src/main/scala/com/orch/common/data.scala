package com.orch.common

object TaskState {
    val NONE     : Int = 0
    val RUNNING  : Int = 1
    val FINISHED : Int = 2
    val FAILURE  : Int = 3
}

object VMState {
    val BOOTING  : Int = 0
    val RUNNING  : Int = 1
    val PAUSE    : Int = 2
    val DOWN     : Int = 3
    val ERROR    : Int = 4
    val KILLED   : Int = 5
    val RESUMING : Int = 6
    val KILLING  : Int = 7
}

object FileState {
    val NO_EXIST : Int = 0
    val SENDING  : Int = 1
    val SENT     : Int = 2
    val FAILURE  : Int = 3
}

object ReplicaType {
    val INPUT  : Int = 0
    val OUTPUT : Int = 1
    val RESULT : Int = 2
}

/**
  * Une tache, represente l'execution d'un executable (e_id)
  * Il est présent dans une VM (v_id), 
  * state est un Int, a cause des codecs, donc 0 -> None, 1 -> Running, 2 -> Finished, 3 -> Failure
  */
case class Task (t_id : Long, v_id : Long, e_id : String, user : String, state : Int, start : Long, end : Long)

/**
  * Un executable, est une élement physique et logique, il représent les fichier binaire d'une tache
  * On suppose ces fichier petit, et par conséquent il sont toujours stocké sur l'orch
  * A chaque execution de tache, on envoi l'executable sur le compute responsable
  */
case class Executable (e_id : String, path : String)

/**
  * Une VM est un élément physique, elle possède plusieurs état
  * On la lance à partir d'un Flavor (f_id)
  * state est un Int à cause des codes, donc 0 -> BOOTING, 1 -> RUNNING, 2 -> PAUSE, 3 -> DOWN, 4 -> ERROR
  */
case class VM (v_id : Long, n_id : String, f_id : Long, user : String, state : Int, start : Long, ready : Long, end : Long)

/**
  * Un noeud, est un compute
  * On stocke son identifiant et ses capacité ainsi que l'adresse de son acteur (daemon)
  * L'acteur sert à effectuer les ordres donnés par le master (orch), ou a recevoir un fichier depuis un autre daemon
  */
case class Node (n_id : String, capas : Map [String, Long], speed : Int, cluster : String, addr : String, port : Int)

/**
  * La bande passante entre les clusters 
  */
case class Cluster (name : String, bw : Map [String, Int])


/**
  * Un fichier est juste un element logique, il sert à representer une dépendance au niveau de l'ordonnanceur
  * Il peut être repliqué plusieurs fois
  * On peut voir cette classe comme, la classe flavor vis à vis de la VM
  */
case class File (f_id : Long, filename: String, user: String, size: Int)

/**
  * Un replica est associé à un fichier, et represente un emplacement réel de fichier (f_id)
  * Un fichier peut ainsi avoir plusieur replica, c'est à l'ordonnanceur de décider ceux qui sont important
  * rtype : 0 -> INPUT, 2 -> OUTPUT
  * state : 0 -> NO_EXIST, 1 -> SENDING, 2 -> SENT
  */
case class Replica (r_id : Long, f_id : Long, n_id : String, t_id : Long, rtype : Int, state : Int, creation : Long, available : Long)

/**
  *  Un IO file est un fichier d'input ou d'output
  *  Il est présent sur le noeud de l'orch / shell, scheduler
  */
case class IOFile (f_id : Long, path : String)

/**
  * Un flavor stocke les informations d'un template de VM
  * On les utilise pour lancer une VM
  */
case class Flavor (f_id : Long, os : String, capas : Map[String, Int], custom_script : String)

/**
  *  Le subscriber est un remote acteur qui s'est inscrit 
  *  On le stocke dans l'hypothèse que l'acteur plante, et que l'on doivent se reconnecter 
  *  En vrai je suis pas sur que ça serve encore avec akka
  */
case class Subscriber (ip : String, port : Int)

/**
  * Une ip est associe a un id de VM
  */
case class Ip (v_id : Long, ip : String)

/**
  *  Juste pour enregister l'instant ou le scheduler est lancé
  */
case class SchedulerInfo (time : Long)
