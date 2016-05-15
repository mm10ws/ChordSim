import akka.actor._
import akka.routing.RoundRobinRouter
import akka.util._
import java.math.BigInteger
import java.util.Base64
import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.net._
import java.io._
import scala.io._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.math._

sealed trait Message
case class boot() extends Message
case class stop() extends Message
case class join(s: ActorRef, p: ActorRef) extends Message
case class joinOp(s: ActorRef, id: Int, starter: Int) extends Message
case class newPred(s: ActorRef) extends Message
case class newSuc(s: ActorRef) extends Message
case class info() extends Message
case class getFingers(b: Int) extends Message
case class getFingerHelper(owner: ActorRef, b: Int) extends Message
case class foundFinger(finger: ActorRef, fid: Int) extends Message
case class forwardSearch(owner: ActorRef, fid: Int) extends Message
case class startQueries(boss: ActorRef, size: Int, numRequests: Int) extends Message
case class found(hops: Int) extends Message
case class forward(boss: ActorRef, query: Int, counter: Int) extends Message

class Node(Id: Int) extends Actor {
  var suc: ActorRef = null
  var pred: ActorRef = null
  var fingerTable = new ArrayBuffer[ActorRef]()
  var numberTable = new ArrayBuffer[Int]()

  var id = 0
  id = Id

  def receive = {
    case join(s, p) =>
      suc = s
      pred = p    

    case joinOp(joinActor, number, starter) =>
      var sint = suc.path.name.toInt
      var pint = pred.path.name.toInt
      if (sint == pint && sint == id) { //only one node        
        suc = joinActor
        pred = joinActor
        joinActor ! newPred(self)
        joinActor ! newSuc(self)
      } else {
        if ((id < number && sint > number) || (id > sint)) {
          //insert here
          suc ! newPred(joinActor)

          joinActor ! newPred(self)
          joinActor ! newSuc(suc)
          suc = joinActor
        } else {
          suc ! joinOp(joinActor, number, starter)
        }
      }

    case newPred(s) =>
      pred = s

    case newSuc(s) =>
      suc = s

    
    case info() => //debug info
      println("id: " + id + " s: " + suc.path.name + " p: " + pred.path.name)
      for (i <- 0 until numberTable.length) {

        print(numberTable(i) + " ")
      }
      println()

    case getFingers(b) =>
      self ! getFingerHelper(self, b)

    case getFingerHelper(owner, b) =>
      var sint = suc.path.name.toInt
      var pint = pred.path.name.toInt
      for (i <- 0 until b) {
        //finger calculation
        var finger = ((Math.pow(2, i) + id) % Math.pow(2, b)).toInt 

        if ((id < finger && finger <= sint) || (sint < id && (id < finger || finger <= sint))) {
          owner ! foundFinger(suc, sint)
        } else {
          suc ! forwardSearch(owner, finger)
        }
      }

    case forwardSearch(owner, finger) =>
      var sint = suc.path.name.toInt
      if ((id < finger && finger <= sint) || (sint < id && (id < finger || finger <= sint))) {
        owner ! foundFinger(suc, sint)
      } else {
        suc ! forwardSearch(owner, finger)        
      }

    case foundFinger(finger, fid) =>
      if (!numberTable.contains(fid)) {
        fingerTable += finger
        numberTable += fid
      }

    case startQueries(boss, size, numRequests) =>
      var sint = suc.path.name.toInt
      //for (i <- 0 until numRequests) {
      var query = Random.nextInt(size)
      var max = -1
      var pos = -1
      for (j <- 0 until numberTable.length) {
        if (numberTable(j) <= query && numberTable(j) > max) {
          max = numberTable(j)
          pos = j
        }
      }
      //println("max: " + max + " q:" + query)

      if (id == query) {
        //done. its me
        boss ! found(1)
      } else if (max == query) {
        //done. its finger  
        boss ! found(1)
      } else if (id < query && id > max) {
        //done. its you
       
        boss ! found(1)
      }       
      else {
        //forward to max finger to find
        if (max == -1) {
          boss ! found(1)

        } else {
          fingerTable(pos) ! forward(boss, query, 2)

        }

      }
   

    case forward(boss, query, counter) =>
      var newCount = counter + 2
      var max = -1
      var pos = -1
      var sint = suc.path.name.toInt
      for (j <- 0 until numberTable.length) {
        if (numberTable(j) <= query && numberTable(j) > max) {
          max = numberTable(j)
          pos = j
        }
      }

      if (id == query) {
        //done its me
        
        boss ! found(newCount)
      } else if (max == query) {
        //done its finger  
        
        boss ! found(newCount)
      } else if (id < query && id > max) {
        //done its you
        
        boss ! found(newCount)
      }     
      
      else {
        //forward to max finger to find
        if (max == -1) {
          println("from " + id + " to " + numberTable(numberTable.length - 1) + " query " + query)
          boss ! found(newCount)
          
        } else {
          
          fingerTable(pos) ! forward(boss, query, newCount)

        }

      }

  }

}

class Boss(numNodes: Int, numRequests: Int) extends Actor {
  var masterNumList = new ArrayBuffer[Int]()
  var masterActorList = new ArrayBuffer[ActorRef]()
  var hopsList = new ArrayBuffer[Int]()
  var doneCount = 0
  
  //generate random nodes
  for (i <- 0 until numNodes) {
    var randomOne = -1
    do {
      randomOne = Random.nextInt(1024) //change to max size 
    } while (masterNumList.contains(randomOne))
    masterNumList += randomOne
  }
  var sortedNumList = scala.util.Sorting.stableSort(masterNumList)

  for (i <- 0 until numNodes) {
    masterActorList += context.actorOf(Props(new Node(sortedNumList(i))), name = sortedNumList(i).toString())
    //masterActorList += context.actorOf(Props(new Node(masterNumList(i))), name = masterNumList(i).toString())
  }

  for (i <- 0 until numNodes) {
    if (i == 0) {
      var s = masterActorList(0)
      masterActorList(i) ! join(s, s) //join for first node
    } else {
      var s = masterActorList(0)
      var p = masterActorList(i - 1)
      //context.system.scheduler.scheduleOnce(50 milliseconds,  masterActorList(0), joinOp(masterActorList(i), masterActorList(i).path.name.toInt, masterActorList(0).path.name.toInt))
      //join nodes
      masterActorList(0) ! joinOp(masterActorList(i), masterActorList(i).path.name.toInt, masterActorList(0).path.name.toInt)
    }
  }

  var b = (Math.log(1000) / Math.log(2)).toInt //change this to reflect the max size
  //println((Math.log(1000)/ Math.log(2)).toInt)

  for (i <- 0 until numNodes) {
    context.system.scheduler.scheduleOnce(50 milliseconds, masterActorList(i), getFingers(10))
  }
  
//  for (i <- 0 until numNodes) {
//    print(sortedNumList(i) + " ")
//        
//  }
//  println()

  def receive = {
    case boot() =>
      //println("boss booted")
      
      for(i <- 0 until numNodes){
        for(j <- 0 until numRequests){
          
           //masterActorList(i) ! startQueries(self, 1024, numRequests) //put in the boss boot method
          context.system.scheduler.scheduleOnce(1000 milliseconds, masterActorList(i), startQueries(self, 1024, numRequests))
        }
       
      }
      

    case found(hops) =>
      doneCount = doneCount + 1
      //println("done " + doneCount)
      hopsList += hops
      if (doneCount >= numNodes * numRequests) {
        var sum:Double = 0
        for(i <- 0 until hopsList.length){
          sum = sum + hopsList(i)
        }
        println("avg # hops: " + sum/hopsList.length)
        self ! stop()
      }

    case stop() =>
      context.stop(self)
      context.system.shutdown()

  }

}

object project3 {

  def main(args: Array[String]) {

    if (args.length == 2) {
      //do work here
      var numNodes = args(0).toInt
      var numRequests = args(1).toInt

      val system = ActorSystem("ChordSystem")
      val boss = system.actorOf(Props(new Boss(numNodes, numRequests)), name = "boss")

      boss ! boot()
      

    } else {
      println("Usage: project3 numNodes numRequests")
    }

  }

}