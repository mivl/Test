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
    /*val edges_1 = edges.map{ line =>
      val fields = line.split(",")
      (fields(1).toLong, fields(0).toLong) //node1_id, edge_id
    }

    val edges_2 = edges.map{ line =>
      val fields = line.split(",")
      (fields(2).toLong, fields(0).toLong) //node2_id, edge_id
    }*/

    val traj_ = traj.map { line =>
      val fields = line.split(" ")
      (fields(0).toLong, (fields(1).toInt, Point(fields(3).toDouble, fields(4).toDouble)))
    }

    val edges_ = edges.map { line =>
      val fields = line.split(",")
      Edge(fields(1).toLong, fields(2).toLong, fields(0).toLong)
    }

    val vert_ = vert.map { line =>
      val fields = line.split(",")
      (fields(0).toLong, Point(fields(1).toDouble, fields(2).toDouble))
    }

    val graph = Graph(vert_, edges_)

    graph.cache()

    //val vertSeq = graph.vertices.map(v => v._1).collect().toSeq
    //val sp = ShortestPaths.run(graph, vertSeq)
    //sp.vertices.saveAsObjectFile("hdfs://ldiag-master:9000/user/isabel/sp-3590")

    val sp = sc.objectFile[(VertexId, ShortestPaths.SPMap)]("hdfs://ldiag-master:9000/user/isabel/sp-3760", 100)
    val sp_ = sp.flatMap(l => l._2.map(m => ((l._1, m._1), m._2)))

    val cand = computeCandidatePoints(traj_, graph.vertices, graph.edges)
    val stm = STMatching(traj_, sp_, cand)
    runTest(stm, vert_, 454)
  }

  def STMatchingReloaded(traj: RDD[(Long, (Int, Point))], sp: RDD[((VertexId, VertexId), Int)],
                 cand: RDD[((Long, Int), Iterable[(Edge[Long], Point)])]): Unit = {

    val tpoint = traj.map{l => ((l._1, l._2._1), l._2._2)}
    val tpointMap = tpoint.collectAsMap() //map from each trajectory point (represented by a pair of Long, Int) to its Point

    val candPoint = cand.join(tpoint).flatMap{l => l._2._1.map{m =>
      (l._1._1, l._1._2, observationProbability(m._2, l._2._2), m._1.srcId, m._2)}
    }.zipWithUniqueId().cache()

    val firstMap = candPoint.map{l => (l._1._1, l._1._2)}.groupByKey().map{l => (l._1, l._2.min)}.collectAsMap() //demorando

    val vertices = candPoint.map{l => if(l._1._2 == firstMap(l._1._1)) (l._2, (l._2, l._1._3)) else (l._2, (None, l._1._3))}

    val temp = candPoint.map{l => ((l._1._1, l._1._2), (l._1._4, l._2))}.cache()

    val tempMap = temp.groupByKey()

    val temp2 = temp.map{case (a, b) => ((a._1, a._2 + 1), b)}
    val joined = temp2.join(tempMap).flatMap{l => l._2._2.map{m => ((l._2._1._1, m._1), ((l._2._1._2, m._2), l._1))}}

    val edges = joined.join(sp).distinct().map{l =>
      val id = l._2._1._2
      val p1 = tpointMap(id._1, id._2 - 1)
      val p2 = tpointMap(id._1, id._2)
      val transProb = euclideanDistance(p1, p2) / l._2._2
      Edge(l._2._1._1._1, l._2._1._1._2, transProb)
    }

  /*  val edges = temp.flatMap{l =>
      if(tempMap.contains(l._1._1, l._1._2 + 1)) {
        val cp = tempMap(l._1._1, l._1._2 + 1)
        cp.map{ m =>
          val p1 = tpointMap(l._1._1, l._1._2)
          val p2 = tpointMap(l._1._1, l._1._2 + 1)
          if(spMap.contains(l._2._1, m._1)) {
            val c1 = l._2._1
            val c2 = m._1
            val transProb = euclideanDistance(p1, p2) / spMap(c1, c2)
            Edge(l._2._2, m._2, transProb)
          } else {
            val transProb = 0.0
            Edge(l._2._2, m._2, transProb)
          }
        }
      } else {
        Iterator.empty
      }
    }
*/
    val new_graph = Graph(vertices, edges)

    val sssp = new_graph.pregel((-1.toLong, -1.toDouble))(
      (id, vd, msg) => {
        if(msg != (-1.toLong, -1.toDouble)) msg else vd
      },
      triplet => {
        if(triplet.srcAttr._1 != None && triplet.dstAttr._1 == None) {
          Iterator((triplet.dstId, (triplet.srcId, triplet.srcAttr._2 + triplet.attr * triplet.dstAttr._2)))
        } else {
          Iterator.empty
        }
      },
      (a, b) => if (math.max(a._2, b._2) == a._2) a else b
    )

    println(sssp.vertices.filter{l => l._2._1 == None}.count())
/*
    //val parent = sssp.vertices.filter(v => v._2._1 != None).map(v => (v._1, v._2._1.asInstanceOf[Long])).collectAsMap() //parent map

    //val vertexToTrajMap = candPoint.map{l => (l._2, l._1._1)}.collectAsMap()
    //val maxKeys = sssp.mapVertices{(id, v) => (vertexToTrajMap(id), v._2)}.vertices.groupBy(_._2._1).map{v => v._2.max}.collectAsMap()
    //val sssp2 = sssp.reverse.mapVertices{(id, v) => if (maxKeys.contains(id)) (true, v._1, Seq(id)) else (false, v._1, Seq(id))}

    val vertexToTraj = candPoint.map{l => (l._2, l._1._1)}
    val maxKeys2 = sssp.vertices.join(vertexToTraj).map{l => (l._1, (l._2._2, l._2._1._2))}.groupBy(_._2._1).map{v => v._2.max}
    val sssp2_v = sssp.vertices.leftJoin(maxKeys2){(id, a, b) =>
      if(b == None) (false, a._1, Seq(id)) else (true, a._1, Seq(id))}//.filter{l => !(l._2._1 == true && l._2._2 == None)}.cache()
    val sssp2_e = sssp.edges.reverse
    val sssp3 = Graph(sssp2_v, sssp2_e)

    //sssp2_v.take(1000).foreach(println)
    //println()
    //sssp3.vertices.take(1000).foreach(println)

    val backtrack = sssp3.pregel(Seq.empty[VertexId])(
      (id, vd, msg) => {
        if(msg == Seq.empty[VertexId]) vd else (true, vd._2, Seq(id) ++ msg)
      },
      triplet => {
        if(triplet.dstId == triplet.srcAttr._2.asInstanceOf[Long] && triplet.srcAttr._1 && !triplet.dstAttr._1) {
          Iterator((triplet.dstId, triplet.srcAttr._3))
        } else {
          Iterator.empty
        }
      },
      (a, b) => a
    )

    //val destVert = sssp2.mapVertices((vid, data) => 0).vertices.minus(sssp2.outDegrees).collectAsMap() //destination vertices
    backtrack.vertices.take(100).foreach(println)

    //backtrack.vertices.filter(v => destVert.contains(v._1) && maxKeys.contains(v._2._3.last)).mapValues(v => v._3).take(5).foreach(println)*/
  }

  def STMatching(traj: RDD[(Long, (Int, Point))], sp: RDD[((VertexId, VertexId), Int)],
                 cand: RDD[((Long, Int), Iterable[(Edge[Long], Point)])]): RDD[(Long, Seq[Edge[Long]])] = {

    val traj_ = traj.map(l => ((l._1, l._2._1), l._2._2))

    val candidatePoints = cand.join(traj_).flatMap(l => l._2._1.map(m =>
      (l._1, observationProbability(m._2, l._2._2), m._1.srcId, m._2, m._1)
    )).zipWithUniqueId().cache()

    val minTimestamp = candidatePoints.map(l => (l._1._1._1, (l._1._1._2, l._2, l._1._2))).groupByKey().flatMap(l => l._2.map(m => (l._1, m, l._2.minBy(_._1)._1)))

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

    //spJoin.map(l => ((l._1._1, l._2._1), l._3)).collect().foreach(println)
    val paths = sssp.vertices.join(trajInd).groupBy(_._2._2).map(l => l._2.maxBy(_._2._1._2)._2).map(l => (l._2, l._1._1))
    val tPtsCount = traj.groupByKey().map(l => (l._1, l._2.size))

    val r = paths.join(tPtsCount).map(l => if(l._2._1.size == l._2._2) (l._1, (l._2._1, true)) else (l._1, (l._2._1, false)))
    val edges1 = candidatePoints.map(l => (l._2, l._1._5)).collectAsMap()
    val f = r.map(l => (l._1, l._2._1.map(m => edges1(m))))
    f
    //r.map(l => l)
    //r.join().map(l => ) join with (cpId, edge)
    //runTest(f)
    //f.collect().foreach(println)
    //println()
    //println(f.filter(l => l._2 == true).count())
    //println(f.filter(l => l._2 == false).count())
  }

  def computeCandidatePoints(traj: RDD[(Long, (Int, Point))], vert: RDD[(Long, Point)],
                             edges: RDD[Edge[Long]]): RDD[((Long, Int), Iterable[(Edge[Long], Point)])] = {

    val treeVert = vert.map(l => (l._2.lat, l._2.lng)).collect()
    val tree = KDTree.fromSeq(treeVert)

    val toVertexId = vert.map(l => (l._2, l._1)).collectAsMap()
    val nearEdges = edges.map(l => (l.srcId, l)).union(edges.map(l => (l.dstId, l))).groupByKey().collectAsMap() //vertice to edge

    val candTemp = traj.map{l =>
      val nearest = tree.findNearest((l._2._2.lat, l._2._2.lng), 5)
      ((l._1, l._2._1), nearest.map(m => toVertexId(Point(m._1, m._2))))
    }

    val candEdges = candTemp.map(l => (l._1, l._2.flatMap(m => nearEdges(m)).distinct))

    val trajMap = traj.map(l => ((l._1, l._2._1), l._2._2)).collectAsMap()
    val pointMap = vert.map(l => (l._1.toLong, l._2)).collectAsMap()
    val edgeToPointMap = edges.map(l => (l, (pointMap(l.srcId), pointMap(l.dstId)))).collectAsMap()

    val candPoints = candEdges.map{l =>
      (l._1, l._2.map{m =>
        val p = trajMap(l._1)
        val c = edgeToPointMap(m)
        val A = c._1
        val B = c._2
        val res = pointToLineSegmentProjection(A, B, p)
        val cp = Point(res(0), res(1))
        (m, cp, euclideanDistance(cp, p))
      })
    }

    val cp2 = candPoints.map(l => (l._1, l._2.toList.sortBy(_._3).take(5).map(m => (m._1, m._2)).toIterable))
    //candPoints.map(l => (l._1, l._2.map(m => (m._1.attr, m._3)))).take(100).foreach(println)
    //println()
    //cp2.map(l => (l._1, l._2.map(m => m._1.attr))).take(100).foreach(println)
    cp2
  }

  def runTest(testRDD: RDD[(Long, Seq[Edge[Long]])], vert: RDD[(Long, Point)], trajIndex: Long): Unit = {
    val vertMap = vert.collectAsMap()
    val testRDD2 = testRDD.map(l => (l._1, l._2.map(m => (vertMap(m.srcId), vertMap(m.dstId)))))
    val f = testRDD2.filter(l => l._1 == trajIndex).first()._2
    println(f)
    vert.count()
  }

  def toPrintable(path: List[Point]): String = {
    path.toSeq.map(p => p.lat + "*" + p.lng).toString().drop(5).dropRight(1)
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
