import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.IntMap
import org.apache.spark.storage.StorageLevel

import Utils._

object EnumerateTriangle{

	def main(args: Array[String]){

        val startTimeMillis = System.currentTimeMillis()
        val delimiter = " "
        //setting up spark environment
        val conf: SparkConf = new SparkConf()
            .setAppName("EnumerateTriangle")
        val sc = new SparkContext(conf)
        // load data from edgelist ListFile
        val lines = sc.textFile(args(0),args(1).toInt)
        val deg=lines.flatMap(line=>{
        	val vertices=line.split(" ")
        	List((vertices(0).toInt, vertices(1).toInt),(vertices(1).toInt, vertices(0).toInt))
        }).distinct().groupByKey().map(a=>{
        	var count=0
        	val items=a._2.iterator
        	for(i<-items)
        	{
        		count=count+1
        	}
        	(a._1,count)
        })
        deg.take(5).foreach(println)

        val edges = lines.map(line =>{
        	val vertices=line.split(" ")
        	if(vertices(0).toInt< vertices(1).toInt)
        		EdgeType(vertices(0).toInt,vertices(1).toInt,-1)
        	else
        		EdgeType(vertices(1).toInt,vertices(0).toInt,-1)
        }).distinct()

        val nei = edges.map(edge=>{(edge.u,edge.v)}).groupByKey().mapValues(_.toArray.sorted)

        nei.take(5).foreach(println)
        //nei.filter(k=>k._2.length>1).map(k=>(k._1,k._2.length)).collect.foreach(println)

        val angles = nei.filter(k=>k._2.length>1).flatMap(n=>{
        	val vertices=n._2
        	// println(vertices.length)
        	for {
        		i <- 0 to vertices.length
        		j <- i+1 to vertices.length-1
        	} 
        	yield EdgeType(vertices(i),vertices(j),n._1)
        })

        println(angles.count())

        // val candTri = angles.map(edge=>{(edge,edge.t)})//.repartitionAndSortWithinPartitions(new EdgePartitioner(args(1).toInt)).groupByKey()
        // val candTri = edges.union(angles).map(edge=>{(edge,edge.t)}).groupByKey().mapValues(_.toArray.sorted)
        // val triangles = candTri.flatMap(ct=>{
        // 	val cand=ct._1
        // 	val it =ct._2.iterator
        // 	if(it.next()== -1)
        // 	{
        // 		for(i<-it)
        // 			yield (cand.u,cand.v,i)
        // 	}
        // 	else
        // 	{
        // 		for (i<-1 to 2)
        // 			yield (cand.u,cand.v,null)
        // 	}
        // }).filter(a=>a._3!=null)
        // //triangles.collect.foreach(println)
        // println(triangles.count())
        // println(candTri.count())
        // candTri.take(2).foreach(println)
    }
}