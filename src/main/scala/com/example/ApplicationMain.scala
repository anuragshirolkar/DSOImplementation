package com.example

import akka.actor.{Actor, Props, ActorSystem}

case object Message
case class SetIndex(ind : Int)
// case class DataPoint(var features : Array[Double], var output : Int)
case class DataPoint(var features : Array[Tuple2[Int, Double]], var output : Int)
case class Data(var dataPoints : Array[DataPoint])
case class AllData(var data : Data, var omega_i : Array[Int], var omega_j : Array[Int])
case class AllDataWithTest(var data : Data, var omega_i : Array[Int], var omega_j : Array[Int], var testData : Data)
case class Weight(weights : Array[Double], weightStart : Int)

object ApplicationMain extends App {
  val clustersCount = 4
  val d = 3231964
  val m = 23961
  val eta : Double = 10.0
  val lmbd : Double = 0.00001

  val system = ActorSystem("MyActorSystem")

  class Master extends Actor {

    var weights = Array.fill[Double](d)(0)
    var reportsCount = 0
    var data : Data = _
    var testData : Data =_
    var epoch :Int = 0
    val start: Long = System.currentTimeMillis
    var omega_i = Array.fill[Int](m)(0)
    var omega_j = Array.fill[Int](d)(0)

    def receive = {
      case AllDataWithTest(data, omega_i, omega_j, testData) => {
        this.data = data
        this.omega_i = omega_i
        this.omega_j = omega_j
        this.testData = testData

        println("data " + data.dataPoints(0).features.deep.mkString(" "))

        println(risk(this.weights, this.data))
        for (worker <- workers) {
          worker ! AllData(data, omega_i, omega_j)
        }
      }

      case Weight(weights, weightStart) => {
        for (i <- 0 to (weights.length-1) ) {
          this.weights(i+weightStart) = (weights(i)+this.weights(i+weightStart))/2
        }
        // println(this.weights.deep.mkString(" "))

        reportsCount += 1
        if (reportsCount == clustersCount) {
          if(this.epoch % 100 == 0) println("epoch result", this.epoch, risk(this.weights, this.data), accuracy(this.weights, this.testData))
          reportsCount = 0
          this.epoch += 1
          if (this.epoch == 10000)
          {
            // println(this.weights.deep.mkString(" "))
            println("final result", risk(this.weights, this.data))
            println("time", System.currentTimeMillis - start)
            context.system.shutdown()
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

    def dot_product(a : Array[Double], b : Array[Tuple2[Int, Double]]) : Double =
    {
      val l = b.length
      var ans : Double = 0
      // println(b.deep.mkString(" "))
      for (i <- 0 to l-1)
      {
        ans += a(b(i)._1)*b(i)._2
      }
      return ans
    }

    def squared_hinge(w : Array[Double], x : Array[Tuple2[Int, Double]], y : Int) : Double =
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
        risk_val += 1.0/m*squared_hinge(weights, data.dataPoints(i).features, data.dataPoints(i).output)
      }
      return risk_val
    }

    def accuracy(weights : Array[Double], testData : Data) : Double =
    {
      var truePos = 0
      var trueNeg = 0
      var falsePos = 0
      var falseNeg = 0
      for (dataPoint <- testData.dataPoints)
      {
        if (dataPoint.output == 1)
        {
          if (dot_product(weights, dataPoint.features) <= 0)
          {
            falseNeg += 1
          }
          else
          {
            truePos += 1
          }
        }
        else
        {
          if (dot_product(weights, dataPoint.features) <= 0)
          {
            trueNeg += 1
          }
          else
          {
            falsePos += 1
          }
        }
      }
      (trueNeg + truePos).toDouble/(trueNeg + falseNeg + truePos + falsePos).toDouble
    }
  }

  class Worker extends Actor {

    var index = -1
    var alphaStart = -1
    var dataPoints = Array[DataPoint]()
    var iteration = 0
    var alpha = Array.fill[Double](m/clustersCount)(0)
    var omega_i = Array.fill[Int](m)(0)
    var omega_j = Array.fill[Int](d)(0)

    def receive = {
      case SetIndex(ind)    => {
        index = ind
        alphaStart = (m/clustersCount) * index
      }
      case AllData(data, omega_i, omega_j) => {
        this.dataPoints = data.dataPoints
        this.omega_i = omega_i;
        this.omega_j = omega_j
      }
      case Message          => {


      }
      case Weight(weights, weightStart)  => {

        // println("signal received by workers", this.index)
        // println("weightStart " + weightStart)
        for (i <- alphaStart to alphaStart + (m/clustersCount)-1)
        {
          for(j <- 0 to data.dataPoints(i).features.length-1)
          {
            if (data.dataPoints(i).features(j)._1 >= weightStart && data.dataPoints(i).features(j)._1 < weightStart + d/clustersCount)
            {
              val index = data.dataPoints(i).features(j)._1
              weights(index-weightStart) -= eta*(lmbd*weights(index-weightStart)/omega_j(index) - alpha(i-alphaStart)*dataPoints(i).features(j)._2/m)
              if (alpha(i-alphaStart) * dataPoints(i).output >= 0)
              {
                alpha(i-alphaStart) += eta*((dataPoints(i).output - alpha(i-alphaStart)/2.0)/(m*omega_i(i)) - weights(index-weightStart)*dataPoints(i).features(j)._2/m)
              }
              else
              {
                alpha(i - alphaStart) = 0
              }
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
        else workers(next) ! Weight(weights, weightStart)
      }
    }




  }

  def getTrainingData() : Data =
  {
    val lines = scala.io.Source.fromFile("data/url_small.tr").mkString
    var dataPoints = new Array[DataPoint](lines.split('\n').length)
    var data = new Data(dataPoints)
    var i = 0
    for(line <- lines.split('\n'))
    {
      val terms = line.split(' ')
      var j = 0
      var dataPoint = new DataPoint(new Array[Tuple2[Int, Double]](terms.length), 0)
      data.dataPoints(i) = dataPoint
      for (term <- terms)
    {
      if (j == 0)
    {
      data.dataPoints(i).output = term.toInt
    }
      else
    {
      val splitTerm = term.split(':')
      data.dataPoints(i).features(j-1) = Tuple2(splitTerm(0).toInt-1, splitTerm(1).toDouble)
    }
      j += 1
    }
      data.dataPoints(i).features(terms.length-1) = Tuple2(d-1, 1.0)
      i += 1
    }
    data
  }

  def getTestData() : Data =
  {

    val lines = scala.io.Source.fromFile("data/url_small.tr").mkString
    var testDataPoints = new Array[DataPoint](lines.split('\n').length)
    var testData = new Data(testDataPoints)
    var i = 0
    for(line <- lines.split('\n'))
    {
      val terms = line.split(' ')
      var j = 0
      val testDataPoint = new DataPoint(new Array[Tuple2[Int, Double]](terms.length), 0)
      testData.dataPoints(i) = testDataPoint
      for (term <- terms)
      {
        if (j == 0)
        {
          testData.dataPoints(i).output = term.toInt
        }
        else
        {
          val splitTerm = term.split(':')
          testData.dataPoints(i).features(j-1) = Tuple2(splitTerm(0).toInt-1, splitTerm(1).toDouble)
        }
        j += 1
      }
      testData.dataPoints(i).features(terms.length-1) = Tuple2(d-1, 1.0)
      i += 1
    }
    testData
  }

  var workers = Array(
    system.actorOf(Props[Worker], "worker1"),
    system.actorOf(Props[Worker], "worker2"),
    system.actorOf(Props[Worker], "worker3"),
    system.actorOf(Props[Worker], "worker4")
  )

  var master = system.actorOf(Props[Master], "master")


  val data = getTrainingData()
  val testData = getTestData()



  var omega_i = Array.fill[Int](m)(0)
  var omega_j = Array.fill[Int](d)(0)
  for (i <- 0 to data.dataPoints.length-1)
  {
    val dataPoint = data.dataPoints(i)
    for (j <- 0 to dataPoint.features.length-1)
    {
      omega_i(i) += 1
      omega_j(dataPoint.features(j)._1) += 1
    }
  }
  // println(omega_i.deep.mkString(" "))
  // println(omega_j.deep.mkString(" "))

  workers(0) ! SetIndex(0)
  workers(1) ! SetIndex(1)
  workers(2) ! SetIndex(2)
  workers(3) ! SetIndex(3)



  master ! AllDataWithTest(data, omega_i, omega_j, testData)

  master ! Message
  system.awaitTermination()
}