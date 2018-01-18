import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.util.collection.{BitSet, OpenHashSet}
import org.apache.spark.rdd.RDD
import scala.collection.immutable.IntMap
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

import Utils._

object EdgeLocalTriangle{

	type VertexSet = OpenHashSet[VertexId]

	def main(args: Array[String]){
        val startTimeMillis = System.currentTimeMillis()
        //setting up spark environment
        val conf: SparkConf = new SparkConf()
            .setAppName("KCore")
        val sc = new SparkContext(conf)
        //val operation="Kclique"
        val graph=GraphLoader.edgeListFile(sc,args(0), true,args(1).toInt,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut).groupEdges((e1, e2) => e1)
     
        val nbrSets: VertexRDD[VertexSet] = graph.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        	val set = new VertexSet(nbrs.length)
        	var i = 0
        	while (i < nbrs.length) {
        		// prevent self cycle
        		if (nbrs(i) != vid) {
        			set.add(nbrs(i))
        		}
        		i += 1
        	}
        	set
        }

        val setGraph = graph.outerJoinVertices(nbrSets) {
        	(vid, _, optSet) => optSet.getOrElse(null)
        }

        val finalGraph= setGraph.mapTriplets(triplet => {
        	val (smallSet, largeSet) = if (triplet.srcAttr.size < triplet.dstAttr.size) {
        		(triplet.srcAttr, triplet.dstAttr)
        	} else {
        		(triplet.dstAttr, triplet.srcAttr)
        	}
        	val iter = smallSet.iterator
        	var counter: Int = 0
        	while (iter.hasNext) {
        		val vid = iter.next()
        		if (vid != triplet.srcId && vid != triplet.dstId && largeSet.contains(vid)) {
        			counter += 1
        		}
        	}
        	counter
        }).triplets.map(triplet=>(triplet.srcId,triplet.dstId,triplet.attr))

        finalGraph.take(10).foreach(println)
        finalGraph.saveAsTextFile(args(2))

        val endTimeMillis = System.currentTimeMillis()
        val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
        println("Total Execution Time : "+durationSeconds.toString() + "s")
        sc.stop()
    }

}