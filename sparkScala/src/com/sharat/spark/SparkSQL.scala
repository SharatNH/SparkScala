package com.sharat.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQL {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  
  def mapper(line:String): Person = {
    val fields = line.split(',')  
    
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
      
    //unstructured data  
    val lines = spark.sparkContext.textFile("../fakefriends.csv")
    
    //if structured data then no need for conversion, directly can be loaded
    // spark.read.json("jsonfilename")
    //RDD
    val people = lines.map(mapper)
    
    //whenever we are inferring the schema of the data, we get cryptic error
    //if we dont have spark.implicit not imported
    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    
    //dataset
    val schemaPeople = people.toDS
    
    schemaPeople.printSchema()
    
    //converts the content of dataset into sql table and names table as "people"
    schemaPeople.createOrReplaceTempView("people")
    
    //at this point we have a sql table created, sitting in memory distributed in
    //cluster
    
    // SQL can be run over DataFrames that have been registered as a table
    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    
    //we get dataframe from the dataset
    val results = teenagers.collect()
    
    results.foreach(println)
    
    spark.stop()
  }
}