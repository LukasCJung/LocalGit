package com.scala.test

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;


object Flights {
   def  main(args: Array[String]) {
      System.setProperty("hadoop.home.dir", "D:\\data\\hadoop-common-2.2.0-bin-master")
      val conf = new SparkConf().setAppName("SparkJoins").setMaster("local").set("spark.executor.memory","1g")

      val sc = new SparkContext(conf)
      
      val input = sc.textFile("D:\\data\\1987.csv\\1987.csv", 1)
      val header = input.first()
      
      //RDD[Array[String]]
      val historys = input.map(line => { line.split(",")}).filter( arrDelayFilter )
        .map(data => ( data(0) + "-" + String.format("%2s",data(1)) + "-" + String.format("%2s",data(2)) , 1))
    
      val count = historys.reduceByKey(_+_)
      count.sortByKey(true, 1).foreach(data => println(data._1 + " : " + data._2) )
           
    println("Hello, world!")
  }
   
   
   def arrDelayFilter( data : Array[String] ) : Boolean =  {
     try{ 
          val time = data(14).toInt
          time > 0
        } catch {
          case e : Exception => false
        }
   }
   
   def depDelayFilter( data : Array[String] ) : Boolean =  {
     try{ 
          val time = data(15).toInt
          time > 0
        } catch {
          case e : Exception => false
        }
   }
   
}
