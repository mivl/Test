import org.apache.spark.{graphx, SparkContext}
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph}
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Stack, Queue, MutableList}

/**
 * Created by isabel on 01/10/15.
 */

object FlowScan {

  //Finds edges that are hot route starts
  def hotRouteStarts(graph: Graph[(String, String), String], traffic: RDD[(String, Iterable[String])],
                     minTraffic: Int): RDD[Array[Edge[String]]] = {

    val trafficMap = traffic.collectAsMap()

    val incOnVrt = graph.collectEdges(EdgeDirection.In).map{l => (l._1.toString, l._2.toIterable)} // (vertex v, set of incident edges on v)
    val srcVertex = graph.edges.map{e => (e.srcId.toString, e)} // (source vertex of e, edge e)
    val incEdgesJoin = srcVertex.join(incOnVrt).map{l => (l._2._1, l._2._2)} // (edge e, set of incident edges on e)

    val xEdges = incEdgesJoin.map{ l =>
      (l._1, l._2.filter{ m =>
        val t_size = trafficMap.get(m.attr).head.size
        t_size >= minTraffic
      })
    }

    val xEdges2 = xEdges.map{ l =>
      val newl1 = trafficMap.get(l._1.attr).head
      (l._1, newl1, l._2.flatMap{ m =>
        trafficMap.get(m.attr).head
      })
    }
    xEdges2.coalesce(1).saveAsTextFile("xedges")

    val n = xEdges2.map{l => (l._1, l._2.toSet.diff(l._3.toSet))}
    val n_filtered = n.filter{l => l._2.size >= minTraffic }

    n_filtered.map{l => Array(l._1)}
  }

  //Returns the set of neighbours that are at a eps-distance
  def epsNeighbourhood(lookup: Map[String, Array[Edge[String]]], baseEdges: RDD[(Edge[String], Array[Edge[String]])],
                       eps: Int): RDD[(Edge[String], Array[Edge[String]])] = {


    if(eps != 0) {
      val edges_ = baseEdges.map{ e =>
        (e._1, e._2 ++ e._2.flatMap{ l =>
          val lu = lookup.get(l.dstId.toString)
          if (lu.isDefined) {
            lu.head
          } else {
            new Array[Edge[String]](0)
          }
        })
      }
      epsNeighbourhood(lookup, edges_, eps-1)
    } else {
      baseEdges.map{ e =>
        var set = e._2.toSet
        if(set.contains(e._1))
          set -= e._1
        (e._1, set.toArray)
      }
    }
  }

  //Returns, for each edge e, the set of edges that are in its eps-neighbourhood and that share at least minTraffic traffic
  def directlyTrafficDensityReachableNeighbours(sc: SparkContext, graph: Graph[(String, String), String], traffic: RDD[(String, Iterable[String])],
                                                neps: RDD[(Edge[String], Array[Edge[String]])],
                                                eps: Int, minTraffic: Int): RDD[(Edge[String], Array[Edge[String]])] = {

    //val trafficSize = traffic.map{l => (l._1, l._2.size)}.collectAsMap()
    val trafficMap = traffic.collectAsMap()

    neps.map{ e =>
      (e._1, e._2.filter{ l =>
        val t_1 = trafficMap(e._1.attr)
        val t_2 = trafficMap(l.attr)
        val t_fin = t_1.toSet.intersect(t_2.toSet)
        t_fin.size >= minTraffic
      })
    }
  }

  def isRouteTrafficDensityReachable(s: Edge[String], r: Array[Edge[String]], graph: Map[graphx.VertexId, Array[Edge[String]]],
                                     dtdr: Map[Edge[String], Array[Edge[String]]], traffic: Map[String, Iterable[String]],
                                     eps: Int, minTraffic: Int): Boolean = {

    var actual = r.last
    val q = mutable.Queue[Edge[String]]()

    var eps_count = 0

    var flag = true

    while(!actual.equals(s) && eps_count < eps && flag) {

      val children = graph(actual.dstId)
      val actual_dtdr = dtdr.get(actual)

      children.foreach(l =>
        if (actual_dtdr.head.contains(l) && traffic(actual.attr).toSet.intersect(traffic(l.attr).toSet).size >= minTraffic) {
        // condition #1 and #2
          q.enqueue(l)
        }
      )
      eps_count = eps_count + 1
      if(q.nonEmpty)
        actual = q.dequeue()
      else
        flag = false
    }

    if (actual.equals(s))
      true
    else
      false
  }

  def extendHotRoutes(r: Array[Edge[String]], graph: Map[graphx.VertexId, Array[Edge[String]]],
                      dtdr: Map[Edge[String], Array[Edge[String]]], traffic: Map[String, Iterable[String]],
                      eps: Int, minTraffic: Int): Array[Edge[String]] = {

    val p = r.last
    val Q = dtdr.get(p)
    if (Q.isDefined) {
      Q.head.foreach{ l => //if Q.head.size > 1... ?
        if(isRouteTrafficDensityReachable(l, r, graph, dtdr, traffic, eps, minTraffic)) {
          val rcopy = r :+ l
          //println(rcopy2.toSeq)
          extendHotRoutes(rcopy, graph, dtdr, traffic, eps, minTraffic)
        }
      }
      //println(r.toSeq)
      r
    } else {
      r
    }
  }

  def extendHotRoutes2(r: Array[Edge[String]], graph: Map[graphx.VertexId, Array[Edge[String]]],
                      dtdr: Map[Edge[String], Array[Edge[String]]], traffic: Map[String, Iterable[String]],
                      eps: Int, minTraffic: Int): ListBuffer[Array[Edge[String]]] = {

    val hotRoutes = new ListBuffer[Array[Edge[String]]]()
    val stack = mutable.Stack[Array[Edge[String]]]()
    stack.push(r)

    while(stack.nonEmpty) {
      val actual = stack.pop()
      val Q = dtdr.get(actual.last)
      if (Q.isDefined) {
        var flag = false
        Q.head.foreach { l =>
          if (isRouteTrafficDensityReachable2(l, actual.last, graph, dtdr, traffic, eps, minTraffic)) {
            val rcopy = actual :+ l
            stack.push(rcopy)
            flag = true
          }
        }
        if(!flag) {
          hotRoutes += actual
        }
      } /*else {
        hotRoutes += actual
      }*/
    }
    hotRoutes
  }

  def isRouteTrafficDensityReachable2(s: Edge[String], r: Edge[String], graph: Map[graphx.VertexId, Array[Edge[String]]],
                                     dtdr: Map[Edge[String], Array[Edge[String]]], traffic: Map[String, Iterable[String]],
                                     eps: Int, minTraffic: Int): Boolean = {

    var actual = r
    val q = mutable.Queue[Edge[String]]()
    var eps_count = 0
    var flag = true

    while(!actual.equals(s) && eps_count < eps && flag) {

      val children = graph.get(actual.dstId).head
      val actual_dtdr = dtdr.get(actual)

      children.foreach(l =>
        if (actual_dtdr.head.contains(l) && traffic(actual.attr).toSet.intersect(traffic(l.attr).toSet).size >= minTraffic) {
          // condition #1 and #2
          q.enqueue(l)
        }
      )
      eps_count = eps_count + 1
      if(q.nonEmpty)
        actual = q.dequeue()
      else
        flag = false
    }

    if (actual.equals(s))
      true
    else
      false
  }

  def run(nodes: RDD[(String, (String, String))], graph: Graph[(String, String), String], traffic: RDD[(String, Iterable[String])],
                      eps: Int, minTraffic: Int, sc: SparkContext): Unit = {

    val lookup = graph.collectEdges(EdgeDirection.Out).map{l => (l._1.toString, l._2)}.collect().toMap
    val baseEdges = sc.parallelize(graph.edges.collect()).map{ l => (l, Array(l))}
    val neps = epsNeighbourhood(lookup, baseEdges, eps)

    val hrs = hotRouteStarts(graph, traffic, minTraffic)
    val dtdr = directlyTrafficDensityReachableNeighbours(sc, graph, traffic, neps, eps, minTraffic).collect().toMap

    val graphEdges = graph.collectEdges(EdgeDirection.Out).collect().toMap
    val trafficMap = traffic.collect().toMap

    val hr = hrs.map{r => extendHotRoutes2(r, graphEdges, dtdr, trafficMap, eps, minTraffic)}
    val hr2 = hr.flatMap{l => l.toSeq.map(i => i.toSeq)}//.coalesce(1).saveAsTextFile("hr")

    val nodesMap = nodes.map{l => (l._1, l._2._1 + "*" + l._2._2)}.collectAsMap()

    //hr2.coalesce(1).saveAsTextFile("hr")

    val out = hr2.map{ l =>
      l.map(m => nodesMap(m.srcId.toString) + ", " + nodesMap(m.dstId.toString))
    }

    val out2 = out.map{l => l.toString().drop(12).dropRight(1)}

    //out2.foreach(println)
    //out2.coalesce(1).saveAsTextFile("out3")
    //hrs.map{l => l.toSeq}.coalesce(1).saveAsTextFile("hrs")
  }
}
