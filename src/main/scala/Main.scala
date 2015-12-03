import com.thesamet.spatial.{DimensionalOrdering, KDTree}
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkFiles, SparkContext, SparkConf}
import org.apache.spark._
import math._
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * Created by isabel on 27/07/15.
 */

object Main {

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

  def main(arg: Array[String]) {
    val startTime = System.currentTimeMillis()

    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)

    val jobName = "EdgeCount"

    //val conf = new SparkConf().setAppName(jobName).setMaster("spark://ldiag-master:7077")//.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.registerKryoClasses(Array(classOf[KDTree]))

    val conf = new SparkConf().setAppName(jobName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val edges = sc.textFile("3760-Edges.txt")
    val nodes = sc.textFile("3760-Nodes.txt")
    val traj = sc.textFile("3760.txt")

    //val edges = sc.textFile("hdfs://ldiag-master:9000/user/lsbd/datasets/3590-Edges.txt")
    //val nodes = sc.textFile("hdfs://ldiag-master:9000/user/lsbd/datasets/3590-Nodes.txt")
    //val traj = sc.textFile("hdfs://ldiag-master:9000/user/lsbd/datasets/3590.txt", 500)

    edges.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)

    val edges_1 = edges.map{ line =>
      val fields = line.split(",")
      (fields(1).toString -> (fields(0), fields(3))) //node1_id -> edge_id, info
    }

    val edges_2 = edges.map{ line =>
      val fields = line.split(",")
      (fields(2).toString -> (fields(0), fields(3))) //node2_id -> edge_id, info
    }

    val nodes_ = nodes.map{ line =>
      val fields = line.split(",")
      (fields(0).toString -> (fields(1), fields(2))) //node_id -> lat, long
    }

    val traj_ = traj.map{ line =>
      val fields = line.split(" ")
      (fields(0).toString -> (fields(3), fields(4))) //traj_id -> lat, long
    }

    val f1 = edges_1.join(nodes_)
    val f2 = edges_2.join(nodes_)

    val f1_new = f1.map{ line =>
      (line._2._1._1 -> (line._1, line._2._2._1, line._2._2._2))
    }

    val f2_new = f2.map{ line =>
      (line._2._1._1 -> (line._1, line._2._2._1, line._2._2._2))
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

    val traffic = fin.union(zeros) // (edge e, set of traj_ids that pass through e)
    //val trafficSize = traffic.map{l => (l._1, l._2.size)}.collectAsMap() // (edge e, number of traj_ids that pass through e)

    val e_ = edges.map{ line =>
      val fields = line.split(",")
      Edge(fields(1).toLong, fields(2).toLong, fields(0))
    }

    val v_ = nodes.map{ line =>
      val fields = line.split(",")
      (fields(0).toLong, (fields(1), fields(2)))
    }

    val graph = Graph(v_, e_)

    val eps = 1
    val minTraffic = 10

    //val fs = FlowScan.run(nodes_, graph, traffic, eps, minTraffic, sc)

    val mm = MapMatching.run(sc, edges, nodes, traj)

    val elapsedTime = System.currentTimeMillis() - startTime
    println("Time: " + elapsedTime + " ms")
  }
}