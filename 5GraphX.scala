// Databricks notebook source
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
val rawData = sc.textFile("/FileStore/tables/3m5tnjo81500699853284/CA_HepTh-9cc05.txt")

// COMMAND ----------

// skip first 4 lines that are comments
val data = rawData.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(4) else iter }

// COMMAND ----------

data.collect().foreach(println)

// COMMAND ----------

// step 1 Define a parser so that you can identify and extract relevant fields
def parse(str: String) = {
    val parts = str.split("\t")
    val src = parts(0).toLong
    val dst = parts(1).toLong
    (src, dst)
}
val parsedData = data.map(parse)
parsedData.collect()

// COMMAND ----------

// As this graph is undirected, I need to remove edge(a, b)= edge(b, a). So I sorted the edges and then remove duplicate
// val rawEdge = parsedData.map{
//   case (src, dst) => if (src < dst) Edge(src, dst, 1) else Edge(dst, src, 1)
// }.distinct.collect

val rawEdge = parsedData.map{
  case (src, dst) => Edge(src, dst, 1) 
}.distinct.collect

// COMMAND ----------

// step 2: Define edge and vertex structure and create property graphs
val edges = sc.parallelize(rawEdge)
val graph = Graph.fromEdges(edges, "defaultProperty")
val vertices = graph.vertices

// COMMAND ----------

println("number of edges: " + graph.edges.count())
println("number of vertices: " + graph.vertices.count())

// COMMAND ----------

// Step 3 
// Qa: Find the nodes with the highest outdegree and find the count of the number of outgoing edges
val Qa = graph.outDegrees.collect.maxBy(_._2)
println("node with highest outdegree: " + Qa._1 + ", count: " + Qa._2)

// COMMAND ----------

//Qb: Find the nodes with the highest indegree and find the count of the number of incoming edges
val Qb = graph.inDegrees.collect.maxBy(_._2)
println("node with highest indegree: " + Qb._1 + ", count: " + Qb._2)

// COMMAND ----------

//Qc.Calculate PageRank for each of the nodes and output the top 5 nodes with the larges PageRank values. You are free to define the threshold parameter.
// Run PageRank
val ranks = graph.pageRank(0.0001).vertices
val top5 =  ranks.sortBy(_._2, false)
top5.collect.take(5)foreach(println)

// COMMAND ----------

//Qd. Run the connected components algorithm on it and find the nodeids of the connected components.
val cc = graph.connectedComponents().vertices
cc.collect

// COMMAND ----------

//Qe. Run the triangle counts algorithm on each of the vertices and output the top 5 vertices with the largest triangle count. In case of ties, you can randomly select the top 5 vertices.
val tc= graph.triangleCount().vertices
tc.sortBy(_._2, false).take(5)

// COMMAND ----------


