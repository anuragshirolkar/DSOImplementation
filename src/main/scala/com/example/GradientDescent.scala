package com.example

/**
 * Created by anuragshirolkar on 29/9/15.
 */


//case class DataPoint(var features : Array[Double], var output : Int)
//case class Data(var dataPoints : Array[DataPoint])
/*
object GradientDescent extends App {
  val clustersCount = 4
  val d = 124
  val m = 1604
  val eta : Double = 0.0001
  val lmbd : Double = 0.00001

  var weights = Array.fill[Double](d)(0)


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
      risk_val += squared_hinge(weights, data.dataPoints(i).features, data.dataPoints(i).output)
    }
    return risk_val
  }



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


  def gradientDescentIteration(): Unit =
  {
    var gradient : Array[Double] = new Array[Double](d)
    for (j <- 0 to d-1)
    {
      var value = lmbd*weights(j)
      for (i <- 0 to m-1)
      {
        val lin_loss = data.dataPoints(i).output*dot_product(weights, data.dataPoints(i).features)
        if (lin_loss < 1)
        {
          var mx = 0.0
          if (1 - lin_loss > mx) mx = 1 - lin_loss
          value -= (1.0/1.0)*2*mx*data.dataPoints(i).output*data.dataPoints(i).features(j)
        }
      }
      gradient(j) = value
    }
    for (j <- 0 to d-1)
    {
      weights(j) -= eta*gradient(j)/5
    }
  }

  val start: Long = System.currentTimeMillis
  for (i <- 0 to 1000)
  {
    if(i % 10 == 0) println("epoch result", i,risk(weights, data))
    gradientDescentIteration()
  }
  println("final result", risk(weights, data))
  println("time", System.currentTimeMillis() - start)
}
*/