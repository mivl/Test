import breeze.linalg._
import breeze.stats.distributions._
import com.thesamet.spatial.KDTree
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeDirection, Edge, Graph}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{Queue, Map}
import scala.math._

/**
 * Created by isabel on 18/11/15.
 */

case class Point(lat: Double, lng: Double) //extends (Double, Double)(lat, lng)

object MapMatching {

  def run(sc: SparkContext, edges: RDD[String], vert: RDD[String], traj: RDD[String]): Unit = {
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
      (fields(0), (fields(1).toInt, Point(fields(3).toDouble, fields(4).toDouble))) //traj_id, timestamp, point
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
    val groupTraj = traj_.groupByKey()
    val cand = computeCandidatePoints(traj_, vert_, e)
    STMatching(graph, traj_, cand)
    cand.count()

  }

  def STMatching(graph: Graph[(String, String), String], traj: RDD[(String, (Int, Point))],
                 cand: RDD[((String, Int), Iterable[(Edge[String], Point)])]): Unit = {

    val tpoint = traj.map{l => ((l._1, l._2._1), l._2._2)} //map from each trajectory point (represented by a pair of String, Int) to its Point
    val candPoint = cand.join(tpoint).flatMap{l => l._2._1.map{m =>
        (l._1._1, l._1._2, m, (observationProbability(m._2, l._2._2)))}
      }.zipWithUniqueId() // ((traj_id, timestamp, candidate p, transmission probability of p), Long)

    val candEges = candPoint.map{l => (l._1._1, (l._1._2, l._2))}.groupByKey()

    val f = traj.groupByKey().first()

    val vertices = candPoint.filter(l => l._1._1 == f._1).map{l => (l._2, l._1._4)} // needs to do distintict()
    
    val temp = candPoint.filter(l => l._1._1 == f._1).map{l => (l._1._2, l._1._3, l._2)}
    val edges = temp.cartesian(temp).filter{case (a,b) => a._1 == b._1 - 1}.map{l =>
      val transProb = transmissionProbability(graph, l._1._2._1, l._2._2._1)
      Edge(l._1._3, l._2._3, transProb)
    }
    edges.foreach(println)

    /*traj.groupByKey().collect().foreach{ t =>
      val vertices = candPoint.filter(l => l._1._1 == t._1).map{l => (l._2, l._1._4)}
      val temp = candPoint.filter(l => l._1._1 == t._1).map{l => (l._1._2, l._1._3, l._2)}
      val edges = temp.cartesian(temp).filter{case (a,b) => a._1 == b._1 - 1}
      edges.foreach(println)
      //val v = sc.parallelize(candPoints.flatMap(p => p._2).toSeq).map(p => (p._2, observationProbability(p._2)))
      //(t._1, t._2.map{ p =>
        //V' = compute candidate points (Point, obsProb) zipwithUniqueIndex?
        //E' = compute edges (?)
        //generate graph G'(V', E')
        //matched_sequence(G')
      //})
    }*/
  }

  def computeCandidatePoints(traj: RDD[(String, (Int, Point))], vert: RDD[(String, Point)],
                             edges: RDD[Edge[String]]): RDD[((String, Int), Iterable[(Edge[String], Point)])] = {
    val vert_ = vert.map{l => (l._2.lat, l._2.lng)}.collect()
    val tree = KDTree.fromSeq(vert_)

    val candTemp = traj.map{ l =>
      val nearest = tree.findNearest((l._2._2.lat, l._2._2.lng), 1).head
      (l._1, l._2._1, Point(nearest._1, nearest._2)) //traj_id, nearestVertex
    }

    val vertMap = vert.map{l => (l._2, l._1)}.collect().toMap
    val candTemp2 = candTemp.map{ l =>
      (l._1, l._2, vertMap(l._3)) //traj_id, node_id
    }

    val eMap = edges.map{l => (l.srcId, l)}.union(edges.map{l => (l.dstId, l)}).groupByKey().collect().toMap

    val candEdges = candTemp2.map{ l =>
      //val r = edgesMap(l._3)
      val r = eMap(l._3.toLong)
      (l._1, l._2, r) //traj_id, timestamp, set of candidate edges
    }

    val trajMap = traj.map{l => ((l._1, l._2._1), l._2._2)}.collect().toMap
    val pointMap = vert.map{l => (l._1.toLong, l._2)}.collect().toMap

    val edgeToPointMap2 = edges.map{l => (l, (pointMap(l.srcId), pointMap(l.dstId)))}.collect().toMap

    val candPoints = candEdges.map{ l =>
      ((l._1, l._2), l._3.map{ m =>
        val p = trajMap(l._1, l._2)
        val c = edgeToPointMap2(m)
        val A = c._1
        val B = c._2
        val res = pointToLineSegmentProjection(A, B, p)
        (m, (Point(res(0), res(1))))
      })
    }
    candPoints
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

  def observationProbability(p1: Point, p2: Point): Double = { //where p1 is a candidate point of trajectory point p2
    val x = euclideanDistance(p1, p2)
    val mean = 0
    val std_dev = 20
    val nd = Gaussian(mean, std_dev)
    nd.pdf(x)
  }

  def transmissionProbability(graph: Graph[(String, String), String], p1: Point, p2: Point, e1: Edge[String], e2: Edge[String]): Double = {
    euclideanDistance(p1, p2)/shortestPathLength(graph, e1, e2).size
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
}
