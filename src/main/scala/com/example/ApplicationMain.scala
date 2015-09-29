package com.example

import akka.actor.{Actor, Props, ActorSystem}

case object Message
case class SetIndex(ind : Int)
case class DataPoint(var features : Array[Double], var output : Int)
case class Data(var dataPoints : Array[DataPoint])
case class Weight(weights : Array[Double], weightStart : Int)

object ApplicationMain extends App {
  val clustersCount = 4
  val d = 124
  val m = 1604
  val eta : Double = 3.0
  val lmbd : Double = 0.00001

  val system = ActorSystem("MyActorSystem")

  class Master extends Actor {

    var weights = Array.fill[Double](d)(0)
    var reportsCount = 0
    var data : Data = _
    var epoch :Int = 0

    def receive = {
      case Data(dataPoints) => {
        this.data = Data(dataPoints)

        println(risk(this.weights, this.data))
        for (worker <- workers) {
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
          println("epoch result", this.epoch, risk(this.weights, this.data))
          reportsCount = 0
          this.epoch += 1
          if (this.epoch == 1000)
          {
            // println(this.weights.deep.mkString(" "))
            println("final result", risk(this.weights, this.data))
          }
          else {
            for (ind <- 0 to clustersCount - 1) {
              // println("sending signal to workers", ind)
              workers(ind) ! Weight(this.weights.slice(ind*d/clustersCount, (ind+1)*d/clustersCount), ind*d/clustersCount)
            }
          }
        }
      }

      case Message => {
        for (ind <- 0 to clustersCount - 1) {
          var weightsNew = Array.fill[Double](d/clustersCount)(0)
          workers(ind) ! Weight(weightsNew, ind*d/clustersCount)
        }
      }
    }

    def dot_product(a : Array[Double], b : Array[Double]) : Double =
    {
      val l = a.length
      var ans : Double = 0
      for (i <- 0 to l-1)
      {
        ans += a(i)*b(i)
      }
      return ans
    }

    def squared_hinge(w : Array[Double], x : Array[Double], y : Int) : Double =
    {
      var mx : Double = 0
      if (1 - y*dot_product(w, x) > 0)
      {
        mx = 1 - y*dot_product(w, x)
      }
      return mx * mx
    }

    def risk(weights : Array[Double], data : Data) : Double =
    {
      var risk_val : Double = 0
      for (i <- 0 to weights.length - 1)
      {
        risk_val += lmbd * weights(i) * weights(i) / 2
      }
      for (i <- 0 to m - 1)
      {
        risk_val += squared_hinge(weights, data.dataPoints(i).features, data.dataPoints(i).output)/m
      }
      return risk_val
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


      }
      case Weight(weights, weightStart)  => {

        // println("signal received by workers", this.index)
        for(j <- 0 to weights.length-1) {
          for (i <- alphaStart to alphaStart + (m/clustersCount)-1)
          {
            weights(j) -= eta/m*(lmbd*weights(j) - alpha(i-alphaStart)*dataPoints(i).features(j+weightStart))
            if (alpha(i-alphaStart) * dataPoints(i).output >= 0)
            {
              alpha(i-alphaStart) += eta*((dataPoints(i).output - alpha(i-alphaStart)/2.0)/(m*d) - weights(j)*dataPoints(i).features(j+weightStart)/m)
            }
            else
            {
              alpha(i - alphaStart) = 0
            }
          }
        }

        var next = this.index - 1
        if (next < 0) next = clustersCount - 1
        iteration += 1
        // println(index, weightStart, weights.deep.mkString(" "))
        if (iteration == clustersCount)
        {
          iteration = 0
          master ! Weight(weights, weightStart)
        }
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

  val lines = scala.io.Source.fromFile("data/a1a.txt").mkString
  var dataPoints = new Array[DataPoint](lines.split('\n').length)
  var data = new Data(dataPoints)
  var i = 0
  for(line <- lines.split('\n'))
  {
    val terms = line.split(' ')
    var j = 0
    var dataPoint = new DataPoint(new Array[Double](d), 0)
    data.dataPoints(i) = dataPoint
    for (term <- terms)
    {
      if (j == 0)
      {
        data.dataPoints(i).output = term.toInt
      }
      else
      {
        var splitTerm = term.split(':')
        data.dataPoints(i).features(splitTerm(0).toInt-1) = splitTerm(1).toDouble
      }
      j += 1
      data.dataPoints(i).features(d-1) = 1
    }
    i += 1
  }

  workers(0) ! SetIndex(0)
  workers(1) ! SetIndex(1)
  workers(2) ! SetIndex(2)
  workers(3) ! SetIndex(3)



  master ! data

  master ! Message
  system.awaitTermination()
}