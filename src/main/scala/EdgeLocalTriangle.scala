import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.util.collection.{BitSet, OpenHashSet}
import org.apache.spark.rdd.RDD
import scala.collection.immutable.IntMap
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

import scala.math._

import Utils._

object EdgeLocalTriangle{

	type VertexSet = OpenHashSet[VertexId]

	case class Edge(u:Long,v:Long,tcount:Int,update:Int)
	case class Triangle(a:Long,b:Long,c:Long)

	def getTriangle[VD: ClassTag, ED: ClassTag](graph:Graph[VD, ED]):Graph[VertexSet, Int]={
    var nbrSets: VertexRDD[VertexSet] = graph.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
      val set = new VertexSet(nbrs.length)
      var i = 0
      while (i < nbrs.length) {
        if (nbrs(i) != vid) {
          set.add(nbrs(i))
        }
        i += 1
      }
      set
    }
    var setGraph = graph.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }
    val finalGraph= setGraph.mapTriplets(triplet => {
      val (smallSet, largeSet) = if (triplet.srcAttr.size < triplet.dstAttr.size) {
        (triplet.srcAttr, triplet.dstAttr)
      } else {
        (triplet.dstAttr, triplet.srcAttr)
      }
      //val participatingSets = new VertexSet(smallSet.size)
      val iter = smallSet.iterator
      var counter: Int = 0
      while (iter.hasNext) {
        val vid = iter.next()
        if (vid != triplet.srcId && vid != triplet.dstId && largeSet.contains(vid)) {
          //participatingSets.add(vid)
          counter=counter+1
        }
      }
      counter
    })

    finalGraph
  }
  def main(args: Array[String]){
	  val startTimeMillis = System.currentTimeMillis()
	  val maxIter = args(2).toInt
	  val partitions = args(1).toInt
	  val conf: SparkConf = new SparkConf().setAppName("EdgeLocalTriangle")
	  val sc = new SparkContext(conf)
	  val graph= GraphLoader.edgeListFile(sc,args(0), true,partitions,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut).removeSelfEdges().groupEdges((e1, e2) => e1)
	  val modifiedGraph= getTriangle(graph)
	  val maxtri=modifiedGraph.triplets.map(a=>{a.attr}).sum
	  val endTimeMillis = System.currentTimeMillis()
	  val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
	  println("Maximum number of Triangles on an Edge: "+(maxtri/3))
	  println("Total Execution Time : "+durationSeconds.toString() + "s")
	  sc.stop()
	}
}