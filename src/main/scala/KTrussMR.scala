import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.util.collection.{BitSet, OpenHashSet}
import org.apache.spark.rdd.RDD
import scala.collection.immutable.IntMap
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.Map
import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer
import scala.math._
import Utils._

object KTrussMR{
	val initialMsg="-10"
	type VertexSet = OpenHashSet[VertexId]
	case class EdgeNew(u:Long,v:Long)
	type EdgeSet = OpenHashSet[EdgeNew]
	def getTriangle[VD: ClassTag, ED: ClassTag](graph:Graph[VD, ED]):Graph[VertexSet, (Int,EdgeSet)]={
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
			val participatingSets = new EdgeSet(smallSet.size)
			val iter = smallSet.iterator
			var counter: Int = 0
			while (iter.hasNext) {
				val vid = iter.next()
				if (vid != triplet.srcId && vid != triplet.dstId && largeSet.contains(vid)) {
					if(vid<triplet.srcId){
						participatingSets.add(EdgeNew(vid,triplet.srcId))
						participatingSets.add(EdgeNew(vid,triplet.dstId))
					}else{
						if(vid<triplet.dstId){
							participatingSets.add(EdgeNew(triplet.srcId,vid))
							participatingSets.add(EdgeNew(vid,triplet.dstId))
						}else{
							participatingSets.add(EdgeNew(triplet.srcId,vid))
							participatingSets.add(EdgeNew(triplet.dstId,vid))
						}
					}
					counter=counter+1
				}
			}
			(counter,participatingSets)
		})
		finalGraph
	}

	def trussCountingMapper(edge:(EdgeNew,(Int,List[EdgeNew],Int)),c:Int):Traversable[(EdgeNew,(Int,List[EdgeNew],Int))]={
		val l =  edge._2._2
		var len = -1
		if(edge._2._1>=c){
			len = l.size-1
		}
		for(i<- -1 to len) yield
		{
			if(i== -1)
			{
				edge
			}else{
			(l(i),(0,List(edge._1),0))
			}
		}
	}

	def reducefunction(e:(EdgeNew,Traversable[(Int,List[EdgeNew],Int)]),c:Int):(EdgeNew,(Int,List[EdgeNew],Int))={
		var l = ArrayBuffer[EdgeNew]()
		var l0 = ArrayBuffer[EdgeNew]()
		val t = e._2
		var s = -1
		for (ee <- t){
			if(ee._1 == 0)
			{
				l0 ++= ee._2
			}else{
				l ++= ee._2
				s = ee._1
			}
		}
		if(s<c)
		{
			(e._1,(s,l.toList,0))
		}else{
			val b= l.intersect(l0)
			val bs=b.size
			val measure =((bs/2)+2)
			if(measure<c)
			{
					(e._1,(c-1,l.toList,1))
			}else
			{
				if(measure==s)
				{
					(e._1,(measure,l.toList,0))
				}else{
					(e._1,(measure,l.toList,1))
				}
			}
		}
	}

	def main(args: Array[String]){
		val startTimeMillis = System.currentTimeMillis()
		val maxIter = args(2).toInt
		//setting up spark environment
		val conf: SparkConf = new SparkConf()
			.setAppName("KTrussMR")
		val sc = new SparkContext(conf)

		var c=2
		var i =0
		var con = true
		val graph= GraphLoader.edgeListFile(sc,args(0), true,args(1).toInt,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut).groupEdges((e1, e2) => e1)
		val modifiedGraph= getTriangle(graph)
		//Might have issue here
		val triEdges=modifiedGraph.edges.map(edge=>{(EdgeNew(edge.srcId,edge.dstId),(edge.attr._1+2,edge.attr._2.iterator.toList,1))})
		var edgemap = triEdges.repartition(args(1).toInt)
		while(i<maxIter && con)
		{
			var change = 1
			while(change!=0)
			{
				edgemap= edgemap.flatMap(e=>trussCountingMapper(e,c)).groupByKey().map(e=>reducefunction(e,c))
				change=edgemap.map(e=>{e._2._3}).sum.toInt
				i=i+1
			}
			val maxc=edgemap.map(e=>{e._2._1}).max
			if(maxc>c)
			{
				c=c+1
			}else{
				con = false
			}
		}
		println("Solution Optimal : "+(!con))
		println("Number of Iterations : "+i)
		println("Maximal k-Truss Value : "+c)
		val endTimeMillis = System.currentTimeMillis()
		val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
		println("Total Execution Time : "+durationSeconds.toString() + "s")
		sc.stop()
	}
}
