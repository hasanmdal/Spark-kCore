import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.util.collection.{BitSet, OpenHashSet}
import org.apache.spark.rdd.RDD
import scala.collection.immutable.IntMap
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.Map
import scala.reflect.ClassTag
import scala.math._
import Utils._

object KTrussDecom{

  val initialMsg="-10"

  type VertexSet = OpenHashSet[VertexId]

  case class Vert(u:Long,v:Long)

  case class EdgeVert(name:String,count:Int)

  case class NewEdge(edge:EdgeVert,tablet:Map[String,Map[String,Int]])

	def getTriangle[VD: ClassTag, ED: ClassTag](graph:Graph[VD, ED]):Graph[VertexSet, (Int,VertexSet)]={
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
      (counter,participatingSets)
    })

    finalGraph
  }

  def getCoUn(first:String, second:String):(String,String) ={
    val a = first.split(",")
    val b = second.split(",")
    if(a.contains(b(0)))
    {
      return (b(0), b(1))
    }
    return (b(1) , b(0))
  }

  def mergeMsgTruss(msg1: String, msg2:String): String = msg1+":"+msg2 // check message

  def vprogTruss(vertexId: VertexId, value: (Vert,Int,Map[String,Map[String,Int]],Int), message: String): (Vert,Int,Map[String,Map[String,Int]],Int) = {
    if (message == initialMsg){
      return (value._1,value._2,value._3,value._4)
    }else{
      var tablet:Map[String,Map[String,Int]]= value._3
      val cTruss=value._2
      val cVert = value._1.u.toString()+","+value._1.v.toString()
      val msg= message.split(":")
      for (m <-msg){
        val (mVert , mTruss) = (m.split("#")(0) , m.split("#")(1).toInt)
        val (co , un) = getCoUn(cVert , mVert)
        if(tablet.contains(un))
        {
          val innerTab = tablet.get(un).head
          if(innerTab.contains(co))
          {
            if(innerTab.get(co).head > mTruss)
            {
              innerTab(co) = mTruss
              tablet(un) = innerTab
            }
          }else
          {
            innerTab += (co -> mTruss)
            tablet(un) = innerTab
          }
        }else{
          val innerTab = Map(co -> mTruss)
          tablet += (un -> innerTab)
        }
      }
      val newTrussness=countTrussness(tablet,cTruss)
      if(newTrussness < cTruss)
      {
        return (value._1,newTrussness,tablet,cTruss)
      }
      return (value._1,value._2,tablet,value._2)
    }
  }

  def countTrussness(M:Map[String,Map[String,Int]],k:Int):Int= {
    var count:Array[Int]=new Array[Int](k+1)
    for ((key,v) <- M)
    {
      if (v.size == 2){
        val tr=v.values.toList
        val j = min(k,min(tr(0),tr(1)))
        count(j)=count(j)+1
      }
    }
    for(i<-k to 3 by -1)
    {
      count(i-1)=count(i-1)+count(i)
    }
    var t=k
    while(t>2 && count(t)<t-2)
    {
      t = t-1
    }
    return t
  }

  def sendMsgTruss(triplet: EdgeTriplet[(Vert, Int , Map[String,Map[String,Int]] , Int), Long]): Iterator[(VertexId, String)] = {
    val sourceVertex = triplet.srcAttr
    val destVertex=triplet.dstAttr
    if(sourceVertex._2!=sourceVertex._4 && destVertex._2!=destVertex._4)
    {
      return Iterator((triplet.dstId,sourceVertex._1.u+","+sourceVertex._1.v+"#"+sourceVertex._2),(triplet.srcId,destVertex._1.u+","+destVertex._1.v+"#"+destVertex._2))
    }else if(sourceVertex._2!=sourceVertex._4){
      return Iterator((triplet.dstId,sourceVertex._1.u+","+sourceVertex._1.v+"#"+sourceVertex._2))
    }else if(destVertex._2!=destVertex._4){
      return Iterator((triplet.srcId,destVertex._1.u+","+destVertex._1.v+"#"+destVertex._2))
    }else{
      return Iterator.empty
    }

  }

  def main(args: Array[String]){
    val startTimeMillis = System.currentTimeMillis()
    val maxIter = args(2).toInt
    val partitions = args(1).toInt
    val conf: SparkConf = new SparkConf().setAppName("KTrussDecom")
    val sc = new SparkContext(conf)
    val graph= GraphLoader.edgeListFile(sc,args(0), true,partitions,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut).removeSelfEdges().groupEdges((e1, e2) => e1)
    val modifiedGraph= getTriangle(graph)
    val eMap:Map[String,Map[String,Int]]=Map()
    val mEdges=modifiedGraph.edges
    val vertLineGraph:VertexRDD[(Vert,Int,Map[String,Map[String,Int]],Int)]= VertexRDD(mEdges.map(edge=>{(Vert(edge.srcId.toLong, edge.dstId.toLong) , edge.attr._1+2 , eMap , 100000)}).zipWithIndex.map(vertex=>{(vertex._2,vertex._1)}))
    val vertMapping: scala.collection.immutable.Map[Vert,Long]=vertLineGraph.map(vert=>{(vert._2._1 , vert._1)}).collectAsMap.toMap
    val broadCastLookupMap = sc.broadcast(vertMapping)
    val edgeLineGraph = mEdges.flatMap(edge=>{
      for(u<-edge.attr._2.iterator) yield
      {
        if(u<edge.srcId)
        {
          (Vert(edge.srcId.toLong , edge.dstId.toLong), Vert(u.toLong, edge.srcId.toLong), Vert(u.toLong, edge.dstId.toLong))
        }else{
          if(u < edge.dstId)
          {
            (Vert(edge.srcId.toLong , edge.dstId.toLong), Vert(edge.srcId.toLong , u.toLong), Vert(u.toLong , edge.dstId.toLong))
          }else{
            (Vert(edge.srcId .toLong, edge.dstId.toLong) , Vert(edge.srcId.toLong , u.toLong), Vert(edge.dstId.toLong , u.toLong))
          }
        }
      }
    }).flatMap(edge=>{
      List(org.apache.spark.graphx.Edge(broadCastLookupMap.value.get(edge._1).head.toLong,broadCastLookupMap.value.get(edge._2).head.toLong,1L),
      org.apache.spark.graphx.Edge(broadCastLookupMap.value.get(edge._1).head.toLong,broadCastLookupMap.value.get(edge._3).head.toLong,1L),
      org.apache.spark.graphx.Edge(broadCastLookupMap.value.get(edge._2).head.toLong,broadCastLookupMap.value.get(edge._3).head.toLong,1L))
    })
    val lineGraph= Graph(vertLineGraph.repartition(partitions), edgeLineGraph.repartition(partitions), (Vert(0L , 0L) , 2 , eMap , 10000000) , StorageLevel.MEMORY_AND_DISK , StorageLevel.MEMORY_AND_DISK).removeSelfEdges().groupEdges((e1, e2) => e1).cache()
    val endGraphTimeMillis = System.currentTimeMillis()
    val durationGraphSeconds = (endGraphTimeMillis - startTimeMillis) / 1000
    val minGraph = lineGraph.pregel(initialMsg,maxIter,EdgeDirection.Both)(vprogTruss,sendMsgTruss,mergeMsgTruss)
    val ktmax=minGraph.vertices.map(v=>{v._2._2}).max
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("Maximal k-Truss Value : "+ktmax)
    println("Line Graph Construction Time : "+durationGraphSeconds.toString() + "s")
    println("Total Execution Time : "+durationSeconds.toString() + "s")
    sc.stop()
  }
}
