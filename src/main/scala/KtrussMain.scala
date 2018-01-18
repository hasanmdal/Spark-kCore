import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.IntMap
import org.apache.spark.storage.StorageLevel

case class ListOwner(nvi:Iterable[Int],owner:Int)
case class Edge(v1:Int,v2:Int)
case class Truss(trussness:Int,edges:Set[Edge],old_trusness:Int)

object KTruss{

    def reduceTriangle(key:(Int,Iterable[ListOwner])) :Int = { return -1 }

    def main(args: Array[String]){
        val startTimeMillis = System.currentTimeMillis()
        //setting up spark environment
        val conf: SparkConf = new SparkConf()
            .setAppName("KTruss")
        val sc = new SparkContext(conf)
        // load data from edgelist ListFile
        val lines = sc.textFile(args(0),args(1).toInt)
        val vnb = lines.flatMap(line =>{List((line.split(" ")(0).toInt, line.split(" ")(1).toInt),(line.split(" ")(1).toInt, line.split(" ")(0).toInt))}).groupByKey(args(1).toInt)
        var k_max = 0
        val triangles=vnb.flatMap(a=>{
            for (i<-a._2) yield {

                (i,ListOwner(a._2,a._1))
            }

        }).union(vnb.map(a=>(a._1,ListOwner(a._2,a._1)))).groupByKey(args(1).toInt).flatMap(a=>{
             val vm=a._1
             val values=a._2
             var tab= Map[Int, Set[Int]]()
             var lvm =Set[Int]()
             for(i <- a._2){
                 if(i.owner==vm)
                 {
                     lvm=i.nvi.toSet
                 }
                 else
                 {
                     tab += (i.owner -> i.nvi.toSet)
                 }
             }
             for ((v,nv) <- tab) yield {
                 val b = nv intersect lvm
                 if(vm>v){
                     (Edge(vm,v),Truss(b.size+2,b.map(e=>{if(vm>e){Edge(vm,e)}else{Edge(e,vm)}}),b.size+2))
                 }
                 else
                 {
                     (Edge(v,vm),Truss(b.size+2,b.map(e=>{if(vm>e){Edge(vm,e)}else{Edge(e,vm)}}),b.size+2))
                 }
             }

         }).reduceByKey((accum, n) => Truss(accum.trussness, accum.edges union n.edges,accum.trussness),args(1).toInt)

         var tr=triangles
         var c=2
         var voteToStop=false
         var iter =0
         var changed =true
         var cs = List[Int]()
         var kmaxs=List[Int]()
         var bol= List[Boolean]()
         var same= List[Boolean]()
         for(it<- 1 to 100){
             iter=iter+1
             var trs= tr.filter(a=>a._2.trussness>=c).flatMap(a=>{
                 val e = a._1
                 val attrib=a._2
                 for (i <- attrib.edges) yield
                 {
                     (i,Truss(0,Set(e),0))
                 }
             }).union(tr).groupByKey(args(1).toInt).map(a=>{
                 val em = a._1
                 val values =a._2
                 var l0= Set[Edge]()
                 var l= Set[Edge]()
                 var S= 0

                 for (i <- values)
                 {
                     if(i.trussness==0)
                     {
                         l0= l0 union i.edges
                     }
                     else
                     {
                         l=i.edges
                         S=i.trussness
                     }
                 }
                if(S<c)
                {
                   (em,Truss(S,l,S))
                }
                else{
                    val b = l intersect l0
                    val tnew= ((b.size)/2)+2
                    if(tnew < c)
                    {
                        (em,Truss(c-1,l,S))
                    }
                    else
                    {
                        (em,Truss(tnew,l,S))
                    }
                }
             })
             tr=trs
             k_max = tr.map(a=>a._2.trussness).max
             kmaxs=kmaxs.:::(List(k_max))
             bol=bol.:::(List(k_max>c))
             cs = cs.:::(List(c))
             val all =tr.filter(a=>a._2.trussness!=a._2.old_trusness).isEmpty()
             same=same.:::(List(all))
             if(all)
             {
                if(k_max > c)
                {
                  c = c + 1
                }else
                {
                  voteToStop=true
                }
            }
        }
        println(c)
        println(iter)
        println(k_max)
        println(cs)
        println(kmaxs)
        println(bol)
        println(same)
        //tr.filter(a=>a._2.trussness==k_max).take(5).foreach(println)
        tr.take(6).foreach(println)
        // triangles.filter(a=>a._1 ==1).take(5).foreach(println)
        sc.stop()
    }
}