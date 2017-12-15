package com.sharat.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

//try later
/* which day had the max precipitation*/
object maxPrecipitation {
  
   def parseIt(lines:String)={
      val fields=lines.split(",")
      val date=fields(1).toInt
      val temperature=fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
      val datatype=fields(2)
      val location=fields(0)
      
      (datatype,location,date,temperature)
   }
   
   def findMax(day1:Int,num1:Float,day2:Int,num2:Float)={
     
       val result=max(num1,num2)
       
       if(result.toFloat==num2)
          (day2,num2)
       else
          (day1,num1)
   }
   
   def main(args:Array[String]){
      
       Logger.getLogger("org").setLevel(Level.ERROR)
       val sc=new SparkContext("local[*]","maxPrecipitation")
       
       val lines=sc.textFile("../1800.csv")
       
       val rdd=lines.map(parseIt)
       
       // all precipitation data
       val prepData=rdd.filter(x=> x._1=="PRCP")
       //output: PRCP date temp
       
       //String (int,float)
       val newPrepData=prepData.map(x=> (x._2,(x._3,x._4.toFloat)))
       
       //give me String, date and maxPrep
       val location_based_prep=newPrepData.reduceByKey((x,y)=> findMax(x._1,x._2,y._1,y._2))
       
       val results=location_based_prep.collect()
       
       for(result<-results){
         
          val location=result._1
          val values=result._2
          val date=values._1
          val temp=values._2
          
          val formattedTemp=f"$temp%.2f"
          
          println(s"$location on $date had $formattedTemp f")
       }
       
   }
}