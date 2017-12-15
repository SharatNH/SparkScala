package com.sharat.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object avgFirstName {
    
  
    def parseIt(input:String)={
      val newLine=input.split(",");
      val firstName=newLine(1).toString();
      val age=newLine(2).toInt
      (firstName,age)
    }
  
  
    def main(args:Array[String]){
       
        Logger.getLogger("org").setLevel(Level.ERROR)
        
        val sc=new SparkContext("local[*]","avgFirstName")
        val lines=sc.textFile("../fakefriends.csv")
        
        // firstname,1
        val rdd=lines.map(parseIt)
        
        //group first name
	      val totalByName=rdd.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
        
	      //finding avg
	      val avgByName=totalByName.mapValues( x=>x._1 / x._2)
        
	      val result=avgByName.collect()
	      //calling action
	      result.sorted.foreach(println)
        
    }
}