package com.sharat.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object popularMovies{
   
    def main(args:Array[String]){
       
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      val sc=new SparkContext("local[*]","popularMovie")
      val lines=sc.textFile("../ml-100k/u.data")
      
      val movies=lines.map(x=>(x.split("\t")(1).toInt,1))
      
      val moviesCount=movies.reduceByKey((x,y)=>(x+y))
      
      val flipseq=moviesCount.map(x=>(x._2,x._1))
      
      val sortedList=flipseq.sortByKey()
      
      val result=sortedList.collect()
      
      result.foreach(println)
    }
}