import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.IntMap
import org.apache.spark.storage.StorageLevel
import scala.math.Ordered.orderingToOrdered

object Utils{

    case class EdgeType(u:Int,v:Int,t:Int){ 
        override def equals(that: Any): Boolean = that match {
            case that:EdgeType => Set(that.u,that.v) == Set(this.u,this.v)
            case _ => false
        }

        override def hashCode: Int = {
            val prime = 31
            var result = 1
            result = prime * result + u;
            result = prime * result + (if (v == null) 0 else v.hashCode)
            return result
        }
    }
        
    object EdgeType{
            implicit def orderingByPivot[A <: EdgeType] : Ordering[A] = {
                Ordering.by(fk => (fk.u, fk.v, fk.t))
            }
        }

    class EdgePartitioner(partitions: Int) extends Partitioner {
    	require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")
    	override def numPartitions: Int = partitions
    	override def getPartition(key: Any): Int = {
    		val k = key.asInstanceOf[EdgeType]
    		k.hashCode() % numPartitions
    	}
    }
}