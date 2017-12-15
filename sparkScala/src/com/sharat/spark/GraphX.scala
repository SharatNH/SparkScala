package com.sharat.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

/** Some examples of GraphX in action with the Marvel superhero dataset! */
object GraphX {
  
  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String) : Option[(VertexId, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      val heroID:Long = fields(0).trim().toLong
      if (heroID < 6487) {  // ID's above 6486 aren't real characters
        return Some( fields(0).trim().toLong, fields(1))
      }
    } 
  
    return None // flatmap will just discard None results, and extract data from Some results.
  }
  
  
  /** Transform an input line from marvel-graph.txt into a List of Edges */
  //superheroId all the connected IDs 
  def makeEdges(line: String) : List[Edge[Int]] = {
    import scala.collection.mutable.ListBuffer
    var edges = new ListBuffer[Edge[Int]]()
    val fields = line.split(" ")
    val origin = fields(0)
    for (x <- 1 to (fields.length - 1)) {
      // Our attribute field is unused, but in other graphs could
      // be used to deep track of physical distances etc.
      edges += Edge(origin.toLong, fields(x).toLong, 0)
    }
    
    return edges.toList
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "GraphX")
    
     // Build up our vertices
    val names = sc.textFile("../marvel-names.txt")
    val verts = names.flatMap(parseNames)
    
    // Build up our edges
    val lines = sc.textFile("../marvel-graph.txt")
    val edges = lines.flatMap(makeEdges)    
    
    // Build up our graph, and cache it as we're going to do a bunch of stuff with it.
    val default = "Nobody" // incase of an edge points to undefined vertex, then it should point to default
    // in our case, point to nobody
    val graph = Graph(verts, edges, default).cache()
    
    // Find the top 10 most-connected superheroes, using graph.degrees:
    println("\nTop 10 most-connected superheroes:")
    // The join merges the hero names into the output; sorts by total connections on each node.
    //returns rdd with all the edge connection to every individual vertex
    // join will give the name of vertex else there will be only vertex ids
    
    graph.degrees.join(verts).sortBy(_._2._1, ascending=false).take(10).foreach(println)


    // Now let's do Breadth-First Search using the Pregel API
    println("\nComputing degrees of separation from SpiderMan...")
    
    // Start from SpiderMan
    val root: VertexId = 5306 // SpiderMan
    
    // Initialize each node with a distance of infinity, unless it's our starting point
    //if spiderman vertex attribute will be set to 0 else vertex attribute(i.e) distance b/w nodes to infinity
    val initialGraph = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)

    // Now the Pregel magic
    //traverse the entire graph and measure the degree of separation to spiderman
    // distance of infinity and max iteration of 10
    val bfs = initialGraph.pregel(Double.PositiveInfinity, 10)( 
        // Our "vertex program" preserves the shortest distance
        // between an inbound message and its current value.
        // It receives the vertex ID we are operating on,
        // the attribute already stored with the vertex, and
        // the inbound message from this iteration.
        (id, attr, msg) => math.min(attr, msg), 
        
        // Our "send message" function propagates out to all neighbors
        // with the distance incremented by one.
        // sends message to neighbouring nodes
        // if incase of actual number, that we havent explored it, it will be 
        //distance +1
        // if infinity that not yet touched yet, then do nothing
        triplet => { 
          if (triplet.srcAttr != Double.PositiveInfinity) { 
            Iterator((triplet.dstId, triplet.srcAttr+1)) 
          } else { 
            Iterator.empty 
          } 
        }, 
        
        // The "reduce" operation preserves the minimum
        // of messages received by a vertex if multiple
        // messages are received by one vertex
        (a,b) => math.min(a,b) ).cache()
    
    // Print out the first 100 results:
        
    //bfs.vertices will get end result of pregal traversal
    //end result contains degree of separation wrt to spiderman
    // join them so that we can get the name of the vertices
        
    bfs.vertices.join(verts).take(100).foreach(println)
    
    // Recreate our "degrees of separation" result:
    println("\n\nDegrees from SpiderMan to ADAM 3,031")  // ADAM 3031 is hero ID 14
    bfs.vertices.filter(x => x._1 == 14).collect.foreach(println)
    
  }
}