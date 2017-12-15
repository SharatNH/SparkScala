package com.sharat.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object friendAgeAvg {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(295); 
  
  def parseIt(line:String)={
  		
  		val fields=line.split(",")
  		
  		val age=fields(2).toInt
  		val count=fields(3).toInt
  		
  		(age,count)
  };System.out.println("""parseIt: (line: String)(Int, Int)""");$skip(466); 
  def main(args: Array[String]){
  		
  		Logger.getLogger("org").setLevel(Level.ERROR)
  		
  		val sc=new SparkContext("local[*]","FriendsByAge")
  		val lines=sc.textFile("../fakefriends.csv")
  		
  		val rdd=lines.map(parseIt)
  		
  		val totalByAge=rdd.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  		
  		val avgAge= totalByAge.mapValues(x=>x._1/x._2)
  		
  		val results=avgAge.collect()
  		
  		results.sorted.foreach(println)
  		
  };System.out.println("""main: (args: Array[String])Unit""")}
  
}
