package com.sharat.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object friendAgeAvg {
  
  // input is line of the type String
  def parseIt(line:String)={		
      //split
  		val fields=line.split(",")	
  		val age=fields(2).toInt
  		val count=fields(3).toInt
  		//return values
  		(age,count)
  }
  
  
  //controls enters here
  def main(args: Array[String]){
  		
      //logger to enter error logs
  		Logger.getLogger("org").setLevel(Level.ERROR)
  		
  		//new sparkContext is started. RDD is created at this point. 
  		//sparkcontext to run in local using all the cores and name of the 
  		//sparkcontext is friendsbyage
  		val sc=new SparkContext("local[*]","FriendsByAge")
  		
  		//the file is input into the new rdd
  		// not a key value pair rdd, just rdd with lines of input
  		val lines=sc.textFile("../fakefriends.csv")
  		
  		//each line of it is extracted using map and passed to parseIt function
  		//the each processed output is then passed to rdd
  		//value returned is in the form of key,value pair
  		val rdd=lines.map(parseIt)
  		
  		//these are common in Scala, multiple line of combined functions
  		//mapValues takes on values, 
  		// 33,45 => 33, (45,1)
  		//reduceByKey will take based on keys and add the values of the key
  		// 33, (45,1)
  		// 33, (50,3)
  		// output 33,(95,4)
  		val totalByAge=rdd.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  		
  		//reduce the value furthur
  		//to find avg divide count/num
  		// 33,(95,4)
  		// output 33, 47.5
  		val avgAge= totalByAge.mapValues(x=>x._1/x._2)
  		
  		//collect is an action
  		//the spark now calcuates graph and decide the plan to
  		// distribute the workload among nodes and perform the above functions
  		val results=avgAge.collect()
  		
  		//call the sorted
  		// and use foreach
  		// print the key value
  		results.sorted.foreach(println)
  		
  }
  
}