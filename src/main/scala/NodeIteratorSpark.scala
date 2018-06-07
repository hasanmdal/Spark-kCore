import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.util.collection.{BitSet, OpenHashSet}
import org.apache.spark.rdd.RDD
import scala.collection.immutable.IntMap
import org.apache.spark.storage.StorageLevel

import scala.util.Sorting._
import scala.reflect.ClassTag

import scala.math._

import Utils._


object NodeIteratorSpark{

    case class Gedge(u:Long,v:Long)

    def main(args: Array[String]){
	  val startTimeMillis = System.currentTimeMillis()
	  val partitions = args(1).toInt
	  val conf: SparkConf = new SparkConf().setAppName("NodeIteratorSpark")
	  val sc = new SparkContext(conf)
	  val graph= GraphLoader.edgeListFile(sc,args(0), true,partitions,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut).removeSelfEdges().groupEdges((e1, e2) => e1)
	  val edges=graph.edges.map(e=>{(Gedge(e.srcId,e.dstId),-1)})
      val triads=graph.collectNeighborIds(EdgeDirection.Out).flatMap(e=>{
          val key=e._1
          val values:Array[Long]=e._2
          quickSort(values)
          for{
              i <- 0 to values.length-1
              j <- (i+1) to values.length-1
            }yield (Gedge(values(i),values(j)),key)
      })
      val triangles=edges.join(triads)
      val tcount=triangles.count();
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("Triangle Count : "+tcount)
      println("Total Execution Time : "+durationSeconds.toString() + "s")
    }
}