import breeze.linalg._
import breeze.stats.distributions._
import com.thesamet.spatial.KDTree
import org.apache.spark.{graphx, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}
import scala.math._

/**
 * Created by isabel on 18/11/15.
 */

case class Point(lat: Double, lng: Double) //extends (Double, Double)(lat, lng)

object MapMatching {

  def run(sc: SparkContext, edges: RDD[String], vert: RDD[String], traj: RDD[String]): Unit = {
    /*val traj_ = traj.map{ line =>
      val fields = line.split(" ")
      (fields(0).toLong, (fields(1).toInt, Point(fields(3).toDouble, fields(4).toDouble)))
    }*/

    val traj_ = traj.map{ line =>
      val fields = line.split(",")
      (fields(0).toLong, Point(fields(3).toDouble, fields(2).toDouble))
    }.zipWithIndex().map(l => (l._1._1, (l._2, l._1._2)))

    val edges_ = edges.map{ line =>
      val fields = line.split(",")
      Edge(fields(1).toLong, fields(2).toLong, 0.toLong)
    }

    val vert_ = vert.map{ line =>
      val fields = line.split(",")
      (fields(0).toLong, Point(fields(1).toDouble, fields(2).toDouble))
    }

    val graph = Graph(vert_, edges_)

    graph.cache()

    /*val vertSeq = graph.vertices.map(v => v._1).collect().toSeq
    val sp = ShortestPaths.run(graph, vertSeq)
    sp.vertices.saveAsObjectFile("hdfs://ldiag-master:9000/user/isabel/sp-beijing")*/

    val sp = sc.objectFile[(VertexId, ShortestPaths.SPMap)]("hdfs://ldiag-master:9000/user/isabel/beijing/sp-beijing", 100)
    val sp_ = sp.flatMap(l => l._2.map(m => ((l._1, m._1), m._2)))

    val cand = computeCandidatePoints(sc, traj_, graph.vertices, graph.edges)

    val traj2 = traj_.map(l => ((l._1, l._2._1), l._2._2))
    val candidatePoints = cand.join(traj2).flatMap(l => l._2._1.map(m =>
      (l._1, observationProbability(m._2, l._2._2), m._1.srcId, m._2, m._1)
    )).zipWithUniqueId().cache()

    STMatching(candidatePoints, traj_, sp_, cand)
    /*val stm2 = STMatching(candidatePoints, traj_, sp_, cand)
    val stm3 = STMatching(candidatePoints, traj_, sp_, cand)

    vert_.count()*/
  }

  def STMatching(candidatePoints: RDD[(((Long, Long), Double, VertexId, Point, Edge[Long]), Long)], traj: RDD[(Long, (Long, Point))], sp: RDD[((VertexId, VertexId), Int)],
                 cand: RDD[((Long, Long), Iterable[(Edge[Long], Point)])]): /*RDD[(Long, Seq[Point])]*/ Unit = {

    /*val traj_ = traj.map(l => ((l._1, l._2._1), l._2._2))

    val candidatePoints = cand.join(traj_).flatMap(l => l._2._1.map(m =>
      (l._1, observationProbability(m._2, l._2._2), m._1.srcId, m._2, m._1)
    )).zipWithUniqueId().cache()*/

    val minTimestamp = candidatePoints.map(l => (l._1._1._1, (l._1._1._2, l._2, l._1._2)))
    .map(l => (l._1, Iterable(l._2))).reduceByKey((a, b) => a ++ b).map(l => (l, l._2.minBy(_._2)._2)).flatMap(l => l._1._2.map(m => (l._1._1, m, l._2)))

    val vertices = minTimestamp.map(l => if(l._2._1 == l._3) (l._2._2, (Seq(l._2._2), l._2._3)) else (l._2._2, (Seq.empty[VertexId], l._2._3)))

    val left = candidatePoints.map(l => (l._1._1, (l._1._3, l._2))).join(traj.map(l => ((l._1, l._2._1), l._2._2))).map(l => (l._1, (l._2._1._1, l._2._1._2, l._2._2)))

    val right = left.groupByKey()

    val cartProd = left.map{case (a, b) => ((a._1, a._2 + 1), b)}.join(right).flatMap(l => l._2._2.map(m => (l._1, l._2._1, m)))

    val spJoin = cartProd.map(l => ((l._2._1, l._3._1), l)).join(sp).map(l => (l._2._1._2, l._2._1._3, l._2._2)).distinct()

    val edges = spJoin.map{l =>
      val p1 = l._1._3
      val p2 = l._2._3
      val transProb = euclideanDistance(p1, p2) / (l._3 + 1)
      Edge(l._1._2, l._2._2, transProb)
    }

    val graph = Graph(vertices, edges)

    val initialMsg = (Seq.empty[VertexId], 0.0)
    val sssp = graph.pregel(initialMsg)(
      (id, vd, msg) => {
        if(msg == initialMsg) vd else (msg._1 :+ id, msg._2)
      },
      triplet => {
        if(triplet.srcAttr._1 != Seq.empty[VertexId] && triplet.dstAttr._1 == Seq.empty[VertexId]) {
          Iterator((triplet.dstId, (triplet.srcAttr._1, triplet.srcAttr._2 + triplet.attr * triplet.dstAttr._2)))
        } else {
          Iterator.empty
        }
      },
      (a, b) => if (math.max(a._2, b._2) == a._2) a else b
    )

    val trajInd = candidatePoints.map(l => (l._2, l._1._1._1))

    val paths = sssp.vertices.join(trajInd).map(l => (l._2._2, l._2._1)).groupByKey().map(l => (l._1, l._2.maxBy(_._2)._1))
    //paths.filter(l => l._1 == 9754).collect().foreach(println)
    //println("---")

    //paths.take(10).foreach(println)

    val tPtsCount = traj.groupByKey().map(l => (l._1, l._2.size))

    val r = paths.join(tPtsCount).map(l => if(l._2._1.size == l._2._2) (l._1, (l._2._1, true)) else (l._1, (l._2._1, false)))
    val edges1 = candidatePoints.map(l => (l._2, l._1._4)).collectAsMap()
    val f = r.map(l => (l._1, l._2._1.map(m => edges1(m))))
    println(toPrintable(f.filter(l => l._1 == 9754).first()._2))
    //println()
    //toPrintable2(traj.groupByKey().filter(l => l._1 == 9754).flatMap(l => l._2)).collect().foreach(println)
    //f
  }

  def computeCandidatePoints(sc: SparkContext, traj: RDD[(Long, (Long, Point))], vert: RDD[(VertexId, Point)],
                             edges: RDD[Edge[Long]]): RDD[((Long, Long), Iterable[(Edge[Long], Point)])] = {

    val treeVert = vert.map(l => (l._2.lat, l._2.lng)).collect()
    val tree = sc.broadcast(KDTree.fromSeq(treeVert))

    val toVertexId = vert.map(l => (l._2, l._1)).collectAsMap()

    val candTemp = traj.map{l =>
      val nearest = tree.value.findNearest((l._2._2.lat, l._2._2.lng), 5)
      ((l._1, l._2._1), nearest.map(m => toVertexId(Point(m._1, m._2))))
    }

    //candTemp.saveAsObjectFile("hdfs://ldiag-master:9000/user/isabel/candTemp")

    val nearEdges = edges.map(l => (l.srcId, l)).union(edges.map(l => (l.dstId, l)))
      .map(l => (l._1, Iterable(l._2))).reduceByKey((a, b) => a ++ b)

    //val candTemp = sc.objectFile[((Long, Long), Seq[Long])]("hdfs://ldiag-master:9000/user/isabel/candTemp", 100)

    val candEdges_ = candTemp.flatMapValues(m => m).map(l => (l._2, l._1)).join(nearEdges) //marked
    val candEdges = candEdges_.map(l => l._2).map(l => (l._1, Iterable(l._2))).reduceByKey((a, b) => a ++ b).flatMapValues(m => m).mapValues(l => l.toSeq.distinct)

    val trajMap = traj.map(l => ((l._1, l._2._1), l._2._2)).collectAsMap()
    val pointMap = vert.map(l => (l._1.toLong, l._2))
    val edgeToPoint = edges.map(l => (l.srcId, l)).join(pointMap).map(l => (l._2._1.dstId, l._2)).join(pointMap).map(l => (l._2._1._1, (l._2._1._2, l._2._2)))

    val ce2 = candEdges.flatMapValues(m => m).map(l => (l._2, l._1)).join(edgeToPoint).map(l => (l._2._1, (l._1, l._2._2))).groupByKey()

    val candPoints = ce2.map{l =>
      (l._1, l._2.map{m =>
        val p = trajMap(l._1)
        val A = m._2._1
        val B = m._2._2
        val res = pointToLineSegmentProjection(A, B, p)
        val cp = Point(res(0), res(1))
        (m._1, cp, euclideanDistance(cp, p))
      })
    }

    val cp2 = candPoints.map(l => (l._1, l._2.toList.sortBy(_._3).take(5).map(m => (m._1, m._2)).toIterable))
    cp2
  }

  def toPrintable(path: Seq[Point]): String = {
    path.map(p => p.lat + "*" + p.lng).toString().drop(5).dropRight(1)
  }

  def toPrintable2(path: RDD[(Long, Point)]): RDD[String] = {
    path.map(p => p._2.lat + ", " + p._2.lng + ", " + p._1)
  }

  def pointToLineSegmentProjection(A: Point, B: Point, p: Point): DenseVector[Double] = {
    val vA = DenseVector(A.lat, A.lng)
    val vB = DenseVector(B.lat, B.lng)
    val vp = DenseVector(p.lat, p.lng)

    val AB = vB - vA
    val AB_squared = AB.t * AB //AB dot AB

    if(AB_squared == 0) { //A and B are the same point
      vA
    } else {
      val Ap = vp - vA
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

  def transmissionProbability(p1: Point, p2: Point, c1: VertexId, c2: VertexId, spMap: mutable.Map[(VertexId, VertexId), Int]): Double = {
    euclideanDistance(p1, p2)/spMap(c1, c2)
  }

  def euclideanDistance(p1: Point, p2: Point): Double = {
    sqrt(pow(p2.lat - p1.lat, 2) + pow(p2.lng - p1.lng, 2))
  }
}
