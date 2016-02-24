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

    val traj_ = traj.map{ line =>
      val fields = line.split(" ")
      (fields(0).toLong, (fields(1).toInt, Point(fields(3).toDouble, fields(4).toDouble))) //traj_id, timestamp, point
    }

    val edges_ = edges.map{ line =>
      val fields = line.split(",")
      Edge(fields(1).toLong, fields(2).toLong, fields(0).toLong)
    }

    val vert_ = vert.map{ line =>
      val fields = line.split(",")
      (fields(0).toLong, Point(fields(1).toDouble, fields(2).toDouble))
    }

    val graph = Graph(vert_, edges_)

    graph.cache()

    val sp = sc.objectFile[(VertexId, ShortestPaths.SPMap)]("hdfs://ldiag-master:9000/user/isabel/sp")
    val sp_ = sp.flatMap(l => l._2.map(m => ((l._1, m._1), m._2)))

    val groupTraj = traj_.groupByKey()
    val cand = computeCandidatePoints(traj_, graph.vertices, graph.edges)
    //val nc = cand.filter(c => c._1._1 == 763).flatMap{c => c._2.map{m => (c._1, m._2)}}
    //nc.collect().foreach(println)

    STMatching(traj_, sp_, cand)
    groupTraj.count()
  }

  def STMatching(traj: RDD[(Long, (Int, Point))], sp: RDD[((VertexId, VertexId), Int)],
                 cand: RDD[((Long, Int), Iterable[(Edge[Long], Point)])]): Unit = {

    val f = traj.groupByKey().filter{l => l._1.equals(294.toLong)}.first()
    val tpoint = traj.map{l => ((l._1, l._2._1), l._2._2)}

    val candPoint = cand.join(tpoint).flatMap{l => l._2._1.map{m =>
        (l._1._1, l._1._2, observationProbability(m._2, l._2._2), m._1.srcId, m._2)}
      }.zipWithUniqueId().filter(l => l._1._1.equals(f._1)) // (traj_id, timestamp, obsProb(p))

    candPoint.cache() //EXTREMAMENTE IMPORTANTE (sem essa persistência, os ids serão recomputados a cada acesso)

    val cpMap = candPoint.map{l => (l._2, l._1._5)}.collectAsMap()

    val startTime = candPoint.map{l => l._1._2}.min()

    val vertices = candPoint.map{l => (l._2, (l._1._3, l._1._2))}./*filter(l => l._2._2 > 2).*/map{l => if(l._2._2 == startTime) (l._1, (l._1, l._2._1)) else (l._1, (None, l._2._1))}

    val tpointMap = tpoint.collectAsMap() //map from each trajectory point (represented by a pair of String, Int) to its Point
    
    val temp = candPoint.map{l => (l._1._2, l._1._4, l._2)}

    val spMap = sp.collectAsMap()

    val edges = temp.cartesian(temp).filter{case (a,b) => a._1 == b._1 - 1}.filter{l => spMap.contains(l._1._2, l._2._2)}.map{l =>
      val c1 = l._1._2
      val c2 = l._2._2
      val p1 = tpointMap(f._1, l._1._1)
      val p2 = tpointMap(f._1, l._2._1)
      val transProb = euclideanDistance(p1, p2)/spMap(c1, c2)
      Edge(l._1._3, l._2._3, transProb)
    }.filter{l => !l.attr.equals(Double.PositiveInfinity)}

    val subgraph = Graph(vertices, edges)

    val sssp = subgraph.pregel((-1.toLong, -1.toDouble))(
      (id, dist, newDist) => {
        if(newDist != (-1.toLong, -1.toDouble)) newDist else dist
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

    val parent = sssp.vertices.filter(v => v._2._1 != None).map(v => (v._1, v._2._1.asInstanceOf[Long])).collectAsMap() //parent map
    val v = sssp.vertices.map(v => (v._2._2, v._1)).max()._2

    var path = new ListBuffer[Long]()
    path += v
    while(parent(path.last) != path.last) {
      path += parent(path.last)
    }

    path.reverse.toList
    val p = path.map{l => cpMap(l)}

    //g.vertices.collect().foreach(println)
    //println()
    //g.edges.collect().foreach(println)

    println(toPrintable(p.reverse.toList))
    println()
    println(toPrintable(f._2.map{l => l._2}.toList))
  }

  def toPrintable(path: List[Point]): String = {
    path.toSeq.map(p => p.lat + "*" + p.lng).toString().drop(5).dropRight(1)
  }

  def computeCandidatePoints(traj: RDD[(Long, (Int, Point))], vert: RDD[(Long, Point)],
                             edges: RDD[Edge[Long]]): RDD[((Long, Int), Iterable[(Edge[Long], Point)])] = {

    val treeVert = vert.map{l => (l._2.lat, l._2.lng)}.collect()
    val tree = KDTree.fromSeq(treeVert)
    val vertMap = vert.map{l => (l._2, l._1)}.collectAsMap()

    /*val candTemp = traj.map{ l =>
      val nearest = tree.findNearest((l._2._2.lat, l._2._2.lng), 1).head
      (l._1, l._2._1, vertMap(Point(nearest._1, nearest._2))) //traj_id, timestamp, nearestVertex_id
    }*/

    val candTemp_ = traj.map{ l =>
      val nearest = tree.findNearest((l._2._2.lat, l._2._2.lng), 6)
      ((l._1, l._2._1), nearest) //traj_id, timestamp, nearestVertex_id
    }

    val candTemp = candTemp_.flatMapValues(v => v).map{v => (v._1._1, v._1._2, vertMap(Point(v._2._1, v._2._2)))}

    val eMap = edges.map{l => (l.srcId, l)}.union(edges.map{l => (l.dstId, l)}).groupByKey().collectAsMap()

    val candEdges = candTemp.map{ l =>
      val r = eMap(l._3.toLong)
      (l._1, l._2, r) //traj_id, timestamp, set of candidate edges
    }

    val trajMap = traj.map{l => ((l._1, l._2._1), l._2._2)}.collectAsMap()
    val pointMap = vert.map{l => (l._1.toLong, l._2)}.collectAsMap()

    val edgeToPointMap2 = edges.map{l => (l, (pointMap(l.srcId), pointMap(l.dstId)))}.collectAsMap()

    val candPoints = candEdges.map{ l =>
      ((l._1, l._2), l._3.map{ m =>
        val p = trajMap(l._1, l._2)
        val c = edgeToPointMap2(m)
        val A = c._1
        val B = c._2
        val res = pointToLineSegmentProjection(A, B, p)
        (m, Point(res(0), res(1)))
      })
    }
    candPoints
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
    //1/(sqrt(2*Pi)*20)*exp(-pow(x,2)/800)
  }

  def transmissionProbability(p1: Point, p2: Point, c1: VertexId, c2: VertexId, spMap: mutable.Map[(VertexId, VertexId), Int]): Double = {
    //euclideanDistance(p1, p2)/shortestPathLength(outEdges, e1, e2).length
    euclideanDistance(p1, p2)/spMap(c1, c2)
  }

  def euclideanDistance(p1: Point, p2: Point): Double = {
    sqrt(pow(p2.lat - p1.lat, 2) + pow(p2.lng - p1.lng, 2))
  }

  /*def shortestPaths(sc: SparkContext, graph: Graph[(String, String), String]): Array[(VertexId, VertexRDD[Double])] = {
    graph.vertices.collect().map{v =>
      val srcId = v._1

      val new_graph = graph.mapVertices((id, _) => if (id == srcId) 0.0 else Double.PositiveInfinity)//.mapEdges(e => 1.0)

      val bfs = new_graph.pregel(Double.PositiveInfinity, 10)(
        (id, attr, msg) => math.min(attr, msg),

        triplet => {
          if (triplet.srcAttr != Double.PositiveInfinity && triplet.dstAttr == Double.PositiveInfinity) {
            Iterator((triplet.dstId, triplet.srcAttr+1))
          } else {
            Iterator.empty
          }
        },

        (a,b) => math.min(a,b)
      )
      (v._1, bfs.vertices)
    }
  }

  def shortestPathLength(outEdges: Array[(graphx.VertexId, Array[Edge[String]])], e1: Edge[String], e2: Edge[String]): Array[Edge[String]] = {
    val queue = Queue[Edge[String]]()
    val path = Map[Edge[String], Array[Edge[String]]]()
    queue.enqueue(e1)
    path.put(e1, Array(e1))
    val oeMap = outEdges.toMap
    var count = 0

    def recursiveSPL(path: Map[Edge[String], Array[Edge[String]]], e2: Edge[String], count: Int): Array[Edge[String]] = {

      if(queue.nonEmpty && path.maxBy(_._2.size)._2.size <= 10) {
        val actual = queue.dequeue()
        if(actual.attr == e2.attr) {
          path(actual)
        } else {
          //val nextSet = graph.edges.filter(e => (e.srcId.equals(actual.dstId)))
          if(oeMap.contains(actual.dstId)) {
            val nextSet = oeMap(actual.dstId)
            nextSet.foreach(e =>
              if(e.attr == e2.attr) {
                path.put(e, path(actual) :+ e)
                return path(e)
              }
            )
            nextSet.foreach(e => queue.enqueue(e))
            nextSet.foreach(e => path.put(e, path(actual) :+ e))
            recursiveSPL(path, e2, count+1)
          } else {
            Array()
          }
        }
      } else {
        Array()
      }
    }
    recursiveSPL(path, e2, count)
  }*/
}
