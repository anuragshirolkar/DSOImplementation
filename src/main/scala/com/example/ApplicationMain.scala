package com.example

import akka.actor.{Actor, Props, ActorSystem}

case object Message
case class SetIndex(ind : Int)
case class DataPoint(features : Array[Double], output : Int)
case class Data(dataPoints : Array[DataPoint])
case class Weight(weights : Array[Double], weightStart : Int)

object ApplicationMain extends App {
  val clustersCount = 4
  val d = 20
  val m = 1000

  val system = ActorSystem("MyActorSystem")

  class Master extends Actor {

    var dataPoints = Array[DataPoint]()
    var weights = Array.ofDim[Double](d)
    var reportsCount = 0

    def receive = {
      case Data(dataPoints) => {
        for (worker <- workers) {
          this.dataPoints = dataPoints
          worker ! Data(dataPoints)
        }
      }

      case Weight(weights, weightStart) => {
        for (i <- 0 to (weights.length-1) ) {
          this.weights(i+weightStart) = weights(i)
        }
        //println(this.weights.deep.mkString(" "))
        reportsCount += 1
        if (reportsCount == clustersCount) {
          println(this.weights.deep.mkString(" "))
        }
      }

      case Message => {
        for (ind <- 0 to clustersCount - 1) {
          var weightsNew = Array.fill[Double](d/clustersCount)(0)
          workers(ind) ! Weight(weightsNew, ind*d/clustersCount)
        }
      }
    }
  }

  class Worker extends Actor {

    var index = -1
    var alphaStart = -1
    var dataPoints = Array[DataPoint]()
    var iteration = 0
    var alpha = Array.fill[Double](m/clustersCount)(0)

    def receive = {
      case SetIndex(ind)    => {
        index = ind
        alphaStart = (m/clustersCount) * index
      }
      case Data(dataPoints) => {
        this.dataPoints = dataPoints
      }
      case Message          => {
        for(dp <- dataPoints) {
          print(dp.output + " ")
          for(f <- dp.features) {
            print(f + " ")
          }
          println()
        }
      }
      case Weight(weights, weightStart)  => {

        //println(index + ", " + iteration + " : " + weights.deep.mkString(" "))
        for(i <- 0 to weights.length-1) {
          weights(i) += index
          // TODO: change weights here
        }
        //println(index + ", " + iteration + " : " + weights.deep.mkString(" "))

        var next = index - 1
        if (next < 0) next = clustersCount - 1
        iteration += 1
        if (iteration == clustersCount) master ! Weight(weights, weightStart)
        else workers.apply(next) ! Weight(weights, weightStart)
      }
    }
  }

  var workers = Array(
    system.actorOf(Props[Worker], "worker1"),
    system.actorOf(Props[Worker], "worker2"),
    system.actorOf(Props[Worker], "worker3"),
    system.actorOf(Props[Worker], "worker4")
  )

  var master = system.actorOf(Props[Master], "master")

  workers(0) ! SetIndex(0)
  workers(1) ! SetIndex(1)
  workers(2) ! SetIndex(2)
  workers(3) ! SetIndex(3)

  var dp = Array(DataPoint(Array(1,2), 1), DataPoint(Array(0, 1), -1))

  master ! Data(dp)

  master ! Message
  system.awaitTermination()
}