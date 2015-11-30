import com.thesamet.spatial.KDTree
import org.apache.spark.graphx
import org.apache.spark.graphx.{EdgeDirection, Graph}
import org.apache.spark.rdd.RDD

/**
 * Created by isabel on 02/10/15.
 */

object SimpleMapMatching {

  //Computes midpoint between two points
  def midpoint(p1: (Double, Double), p2: (Double, Double)): (Double, Double) = {
    val lat1 = math.toRadians(p1._1)
    val lon1 = math.toRadians(p1._2)
    val lat2 = math.toRadians(p2._1)
    val lon2 = math.toRadians(p2._2)

    val bx = math.cos(lat2) * math.cos(lon2 - lon1)
    val by = math.cos(lat2) * math.sin(lon2 - lon1)
    val lat3 = math.atan2(math.sin(lat1) + math.sin(lat2), math.sqrt((math.cos(lat1) + bx) * (math.cos(lat1) + bx) + by*by))
    val lon3 = lon1 + math.atan2(by, math.cos(lat1) + bx)

    (Math.toDegrees(lat3), Math.toDegrees(lon3))
  }

  def run(edges: RDD[String], vert: RDD[String], traj: RDD[String]): RDD[(String, Iterable[String])] = {

    val edges_1 = edges.map{ line =>
      val fields = line.split(",")
      (fields(1), fields(0)) //node1_id -> edge_id
    }

    val edges_2 = edges.map{ line =>
      val fields = line.split(",")
      (fields(2), fields(0)) //node2_id -> edge_id
    }

    val vert_ = vert.map{ line =>
      val fields = line.split(",")
      (fields(0), (fields(1), fields(2))) //node_id -> lat, long
    }

    val traj_ = traj.map{ line =>
      val fields = line.split(" ")
      (fields(0), (fields(3), fields(4))) //traj_id -> lat, long
    }

    val f1 = edges_1.join(vert_)
    val f2 = edges_2.join(vert_)

    val f1_new = f1.map{ l =>
      (l._2._1, (l._1, l._2._2._1, l._2._2._2))
    }

    val f2_new = f2.map{ l =>
      (l._2._1, (l._1, l._2._2._1, l._2._2._2))
    }

    val f = f1_new.union(f2_new)
    val f_new = f.groupByKey()

    val midp = f_new.map{ l =>
      val temp = midpoint((l._2.head._2.toDouble, l._2.head._3.toDouble), (l._2.last._2.toDouble, l._2.last._3.toDouble))
      ((temp._1, temp._2), l._1)
    }

    val m = midp.map{ l =>
      (l._1._1, l._1._2)
    }.collect()

    //val g = graph.mapEdges(l => 0) //zera os pesos das arestas

    val tree = KDTree.fromSeq(m)

    val t = traj_.map{ x =>
      (tree, (x._2._1.toDouble, x._2._2.toDouble), x._1)
    }

    val temp = t.map{ l =>
      (l._1.findNearest(l._2, 1).head, l._3)
    }

    val s = temp.distinct().groupByKey()

    val r = temp.distinct().map{l => (l._1, 1)}.reduceByKey{case (x, y) => x + y}

    val zeros = midp.subtractByKey(r).map{l => (l._2, Iterable[String]())}

    val fin = midp.join(s).map{l => l._2}

    val traffic = fin.union(zeros) // (edge e, set of traj_ids that pass through e

    traffic
    //val trafficSize = traffic.map{l => (l._1, l._2.size)}.collectAsMap() // (edge e, number of traj_ids that pass through e)
  }

}
