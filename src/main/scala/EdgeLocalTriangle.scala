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

	def identifyKtruss[VD: ClassTag, ED: ClassTag](graph:Graph[VertexSet, Int],k:Int):Graph[VertexSet, Int]={

		if(k>0){
			graph.subgraph(vpred=(id,attr)=>attr.size!=0,epred = (triplet) => triplet.attr > k)
		}

		var nbrSets: VertexRDD[VertexSet] = graph.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
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

        var setGraph = graph.outerJoinVertices(nbrSets) {
        	(vid, _, optSet) => optSet.getOrElse(null)
        }

        val finalGraph= setGraph.mapTriplets(triplet => {
        	val (smallSet, largeSet) = if (triplet.srcAttr.size < triplet.dstAttr.size) {
        		(triplet.srcAttr, triplet.dstAttr)
        	} else {
        		(triplet.dstAttr, triplet.srcAttr)
        	}
        	val participatingSets = new VertexSet(smallSet.size)
        	val iter = smallSet.iterator
        	var counter: Int = 0
        	while (iter.hasNext) {
        		val vid = iter.next()
        		if (vid != triplet.srcId && vid != triplet.dstId && largeSet.contains(vid)) {
        			participatingSets.add(vid)
        			counter=counter+1
        		}
        	}
        	counter
        })
        finalGraph
	}

	def main(args: Array[String]){
        val startTimeMillis = System.currentTimeMillis()
        //setting up spark environment
        val conf: SparkConf = new SparkConf()
            .setAppName("KCore")
        val sc = new SparkContext(conf)
        //val operation="Kclique"
        var graph=GraphLoader.edgeListFile(sc,args(0), true,args(1).toInt,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut).groupEdges((e1, e2) => e1)
        
        var nbrSets: VertexRDD[VertexSet] = graph.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
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

        var setGraph = graph.outerJoinVertices(nbrSets) {
        	(vid, _, optSet) => optSet.getOrElse(null)
        }

        var modifiedGraph= setGraph.mapTriplets(triplet => {
        	val (smallSet, largeSet) = if (triplet.srcAttr.size < triplet.dstAttr.size) {
        		(triplet.srcAttr, triplet.dstAttr)
        	} else {
        		(triplet.dstAttr, triplet.srcAttr)
        	}
        	val participatingSets = new VertexSet(smallSet.size)
        	val iter = smallSet.iterator
        	var counter: Int = 0
        	while (iter.hasNext) {
        		val vid = iter.next()
        		if (vid != triplet.srcId && vid != triplet.dstId && largeSet.contains(vid)) {
        			participatingSets.add(vid)
        			counter=counter+1
        		}
        	}
        	counter
        })

        var ePres= true
        var minTrusness=0
        var i =1
        var count:Long=1
        var counts:List[Long]=List(modifiedGraph.triplets.count())
        var tripCounts:List[Long]=List(-1)
        var tripCounts2:List[Long]=List(-1)
        var filterEdg=modifiedGraph.edges.filter(triplet=>triplet.attr > minTrusness)
        		var filterVertex=modifiedGraph.triplets.filter(triplet=>triplet.attr > minTrusness).flatMap(triplet=>{
        			List(triplet.dstId,triplet.srcId)
        			}).distinct()
        var newGraph=Graph.fromEdges(filterEdg,null)
     	while(ePres && i<args(2).toInt){
        	if(newGraph.edges.isEmpty)
        	{
        		ePres = false
        	}else{
        		count=newGraph.triplets.count()
        		counts=count::counts
        		nbrSets = newGraph.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
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

        		setGraph = newGraph.outerJoinVertices(nbrSets) {
        			(vid, _, optSet) => optSet.getOrElse(null)
        		}
        		//setGraph=setGraph.subgraph(vpred=(id, attr) => attr.size>1)
        		//tripCounts=setGraph.triplets.count()::tripCounts
        		modifiedGraph= setGraph.mapTriplets(triplet => {
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
        		val c_minTrusness=modifiedGraph.triplets.map(triplet=>triplet.attr).min
        		if(c_minTrusness>minTrusness)
        		{
        			minTrusness=c_minTrusness
        		}
        		tripCounts=minTrusness::tripCounts
        		filterEdg=modifiedGraph.edges.filter(triplet=>triplet.attr > minTrusness)
        		if(filterEdg.isEmpty)
        		{
        			ePres = false
        		}else
        		{
        		newGraph=Graph.fromEdges(filterEdg,null)
        		}
        	}
        	i=i+1
        	println(i)
     	}
        println(ePres)
        println(i)
        println(minTrusness)
        println("COUNTS EDGES")
        counts.foreach(println)
        println("COUNTS EDGES setGraph")
        tripCounts.foreach(println)
        println("COUNTS EDGES mod graph")
        tripCounts2.foreach(println)
        // finalGraph.saveAsTextFile(args(2))

        val endTimeMillis = System.currentTimeMillis()
        val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
        println("Total Execution Time : "+durationSeconds.toString() + "s")
        sc.stop()
    }

}