import breeze.linalg._
import breeze.stats.distributions._
import com.thesamet.spatial.KDTree
import org.apache.spark.graphx.{EdgeDirection, Edge, Graph}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{Queue, Map}
import scala.math._

/**
 * Created by isabel on 18/11/15.
 */

case class Point(lat: Double, lng: Double) //extends (Double, Double)(lat, lng)

object MapMatching {

  def run(edges: RDD[String], vert: RDD[String], traj: RDD[String]): Unit = {
    val edges_1 = edges.map{ line =>
      val fields = line.split(",")
      (fields(1), fields(0)) //node1_id, edge_id
    }

    val edges_2 = edges.map{ line =>
      val fields = line.split(",")
      (fields(2), fields(0)) //node2_id, edge_id
    }

    val vert_ = vert.map{ line =>
      val fields = line.split(",")
      (fields(0), Point(fields(1).toDouble, fields(2).toDouble)) //node_id, point
    }

    val traj_ = traj.map{ line =>
      val fields = line.split(" ")
      (fields(0), fields(1).toInt, Point(fields(3).toDouble, fields(4).toDouble)) //traj_id, timestamp, point
    }

    val e = edges.map{ line =>
      val fields = line.split(",")
      Edge(fields(1).toLong, fields(2).toLong, fields(0))
    }

    val v = vert.map{ line =>
      val fields = line.split(",")
      (fields(0).toLong, (fields(1), fields(2)))
    }

    val graph = Graph(v, e)

    val e1 = graph.edges.filter(e => e.attr == "571036913").first()
    val e2 = graph.edges.filter(e => e.attr == "250737111").first()

    val spl = shortestPathLength(graph, e1, e2)
    println(spl.toSeq)

    //val groupTraj = traj_.groupByKey()

    //computeCandidatePoints(traj_, vert_, edges_1.union(edges_2))
    traj_.count()
  }

  def computeCandidatePoints(traj: RDD[(String, Int, Point)], vert: RDD[(String, Point)],
                             edges: RDD[(String, String)]): Unit = {
    val vert_ = vert.map{l => (l._2.lat, l._2.lng)}.collect()
    val tree = KDTree.fromSeq(vert_)

    val candTemp = traj.map{ l =>
      val nearest = tree.findNearest((l._3.lat, l._3.lng), 1).head
      (l._1, l._2, Point(nearest._1, nearest._2)) //traj_id, nearestVertex
    }

    val vertMap = vert.map{l => (l._2, l._1)}.collect().toMap
    val candTemp2 = candTemp.map{ l =>
      (l._1, l._2, vertMap.get(l._3).head) //traj_id, node_id
    }

    val edgesMap = edges.groupByKey().collect().toMap

    val candEdges = candTemp2.map{ l =>
      val r = edgesMap.get(l._3).head
      (l._1, l._2, r) //traj_id, timestamp, edge_id set of candidate edges
    }

    val trajMap = traj.map{l => ((l._1, l._2), l._3)}.collect().toMap
    val vertMap2 = vert.collect().toMap
    val edgeToPointMap = edges.map{l => (l._2, l._1)}.groupByKey().map{ l =>
      (l._1, l._2.map{m =>
        vertMap2.get(m).head
      })}.collect().toMap

    val candPoints = candEdges.map{ l =>
      (l._1, l._2, l._3.map{ m =>
        val p = trajMap.get(l._1, l._2).head
        val c = edgeToPointMap.get(m).head
        val A = c.head
        val B = c.last
        val res = pointToLineSegmentProjection(A, B, p)
        (m, Point(res(0), res(1)))
      })
    } //traj_id, timestamp, set of (candEdge, Point)
    candPoints.foreach(println)
  }

  def pointToLineSegmentProjection(A: Point, B: Point, p: Point): DenseVector[Double] = {
    val vA = DenseVector(A.lat, A.lng)
    val vB = DenseVector(B.lat, B.lng)
    val vp = DenseVector(p.lat, p.lng)

    val AB = (vB-vA)
    val AB_squared = AB.t * AB //AB dot AB

    if(AB_squared == 0) { //A and B are the same point
      vA
    } else {
      val Ap = (vp-vA)
      val t = (Ap.t * AB) / AB_squared
      if (t < 0.0)
        vA
      else if (t > 1.0)
        vB
      else
        vA + t * AB
    }
  }

  def observationProbability(x: Double): Double = {
    val mean = 0
    val std_dev = 20
    val nd = Gaussian(mean, std_dev)
    nd.pdf(x)
  }

  def transmissionProbability(graph: Graph[(String, String), String], p1: Point, p2: Point, c1: Point, c2: Point): Double = {
    //euclideanDistance(p1, p2)/shortestPathLength(graph, e1, e2, 0)
    0
  }

  def euclideanDistance(p1: Point, p2: Point) = {
    sqrt(pow((p2.lat - p1.lat), 2) + pow((p2.lng - p1.lng), 2))
  }

  def shortestPathLength(graph: Graph[(String, String), String], e1: Edge[String], e2: Edge[String]): Array[Edge[String]] = {
    val queue = Queue[Edge[String]]()
    val path = Map[Edge[String], Array[Edge[String]]]()
    queue.enqueue(e1)
    path.put(e1, Array(e1))

    def recursiveSPL(graph: Graph[(String, String), String], queue: Queue[Edge[String]],
                     path: Map[Edge[String], Array[Edge[String]]], e2: Edge[String]): Array[Edge[String]] = {
      if(queue.nonEmpty && path.maxBy(_._2.size)._2.size <= 10) {
        val actual = queue.dequeue()
        if(actual.attr == e2.attr) {
          path(actual)
        } else {
          val nextSet = graph.edges.filter(e => e.srcId == actual.dstId).collect()
          nextSet.foreach(e =>
            if(e.attr == e2.attr) {
              path.put(e, path(actual) :+ e)
              return path(e)
            }
          )
          nextSet.foreach(e => queue.enqueue(e))
          nextSet.foreach(e => path.put(e, path(actual) :+ e))
          recursiveSPL(graph, queue, path, e2)
        }
      } else Array()
    }
    recursiveSPL(graph, queue, path, e2)
  }

  /*def shortestPathLength(graph: Graph[(String, String), String], e1: Edge[String], e2: Edge[String]): Int = {
    val queue = Queue[Edge[String]]()
    val pathLength = Map[Edge[String], Int]()
    val parent = Map[Edge[String], Edge[String]]()
    queue.enqueue(e1)
    pathLength.put(e1, 0)

    def recursiveSPL(graph: Graph[(String, String), String], queue: Queue[Edge[String]], pathLength: Map[Edge[String], Int], e2: Edge[String]): Int = {
      if(queue.nonEmpty && pathLength.maxBy(_._2)._2 <= 10) {
        val actual = queue.dequeue()
        if(actual.attr == e2.attr) {
          pathLength(actual)
        } else {
          val nextSet = graph.edges.filter(e => e.srcId == actual.dstId).collect()
          nextSet.foreach(e =>
            if(e.attr == e2.attr)
              return pathLength(actual)+1
          )
          nextSet.foreach(e => queue.enqueue(e))
          nextSet.foreach(e => pathLength.put(e, pathLength(actual)+1))
          nextSet.foreach(e => parent.put(e, actual))
          recursiveSPL(graph, queue, pathLength, e2)
        }
      } else -1
    }
    recursiveSPL(graph, queue, pathLength, e2)
  }*/
}
