package org.jfratzke.spark.utils

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Extra functions available on RDDs of (key, value) pairs through an implicit conversion.
  * TODO: Add Right Outer + Full Outer
  */
class SkewPairRDDFunctions[K : ClassTag : Ordering, V : ClassTag](self: RDD[(K, V)])
  extends Serializable {

  /**
    * High Skew Join Problem Work-Around. The traditional approach has been to increase executor memory. However, that
    * very obviously isn't a real solution. This attempts to remedy the problem by replicating the right side of the join
    * and key by an index which should allow Spark to more evenly distribute the job into separate tasks.
    */
  private def replicatedSkewJoinPrep[R : ClassTag](right: RDD[(K, R)]): (RDD[((Int, K), V)], RDD[((Int, K), R)]) = {
    val leftCounts = self.map{ case (k, v) => (k, 1) }.reduceByKey(_ + _)

    val expanded = right
      .leftOuterJoin(leftCounts)
      .repartition(self.getNumPartitions)
      .flatMap{
        case (key, values) =>
          val (item, count) = values
          (0 until count.getOrElse(1)).map(x => ((x, key), item))
      }

    val indexKeyed =
      self
        .groupByKey()
        .flatMap{
          case (key, iter) =>
            val size = iter.size
            (0 until size).zip(iter).map(x => ((x._1, key), x._2))

        }
    (indexKeyed, expanded)
  }

  def skewedJoin[R : ClassTag](right: RDD[(K, R)]): RDD[(K, (V , R))] = {
    val (l, r) = replicatedSkewJoinPrep(right)
    l.join(r).map {
      case (key, value) =>
        (key._2, (value._1, value._2))
    }
  }

  def skewedLeftOuterJoin[R : ClassTag](right: RDD[(K, R)]): RDD[(K, (V , Option[R]))] = {
    val (l, r) = replicatedSkewJoinPrep(right)
    l.leftOuterJoin(r).map {
      case (key, value) =>
        (key._2, (value._1, value._2))
    }
  }

  def skewedRightOuterJoin[R : ClassTag](right: RDD[(K, R)]): RDD[(K, (Option[V] , R))] = {
    val (l, r) = replicatedSkewJoinPrep(right)
    l.rightOuterJoin(r).map {
      case (key, value) =>
        (key._2, (value._1, value._2))
    }
  }

  def skewedFullOuterJoin[R : ClassTag](right: RDD[(K, R)]): RDD[(K, (Option[V] , Option[R]))] = {
    val (l, r) = replicatedSkewJoinPrep(right)
    l.fullOuterJoin(r).map {
      case (key, value) =>
        (key._2, (value._1, value._2))
    }
  }


}

object DBPairRDDFunctions {
  implicit def rddToDBPairRDDFunctions[K : ClassTag : Ordering, V : ClassTag](rdd: RDD[(K, V)]): SkewPairRDDFunctions[K, V] = {
    new SkewPairRDDFunctions(rdd)
  }
}
