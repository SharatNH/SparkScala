package com.sharat.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object avgExpendofCustomer {
   
    def parseIt(input:String)={
       val fields=input.split(",")
       val customerId=fields(0).toInt
       val amount=fields(2).toFloat
       (customerId,amount)
    }
    
    def main(args:Array[String]){
    
      Logger.getLogger("org").setLevel(Level.ERROR)
      val sc=new SparkContext("local[*]","avgExpendofCustomer")
      
      val lines=sc.textFile("../customer-orders.csv")
      
      val rdd=lines.map(parseIt)
       
      val instanceCount=rdd.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      
      val avgAmount=instanceCount.mapValues(x=>(x._1/x._2))
      
      
      val total_per_customer=rdd.reduceByKey((x,y)=>x+y)   
      val amount_customer=total_per_customer.map(x=>(x._2,x._1))
          
 
      
      val results=avgAmount.collect()
      val results_total=amount_customer.collect()
   
      results.sorted.foreach(println)
      
      println("//////////////////////////////////")
      
      results_total.sorted.foreach(println)
      
      
      
    }
}