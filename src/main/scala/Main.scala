import com.thesamet.spatial.{DimensionalOrdering, KDTree}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark._
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

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val jobName = "Distances"

    val conf = new SparkConf().setAppName(jobName).setMaster("spark://ldiag-master:7077").set("spark.executor.memory", "6g").set("spark.driver.memory", "2g")

    //val conf = new SparkConf().setAppName(jobName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    //sc.addJar("/home/isabel/IdeaProjects/Test/target/scala-2.10/Test-assembly-1.0.jar")

    //val edges = sc.textFile("3760-Edges.txt")
    //val nodes = sc.textFile("3760-Nodes.txt")
    //val traj = sc.textFile("3760.txt")

    val edges = sc.textFile("hdfs://ldiag-master:9000/user/isabel/3760-Edges.txt")
    val nodes = sc.textFile("hdfs://ldiag-master:9000/user/isabel/3760-Nodes.txt")
    val traj = sc.textFile("hdfs://ldiag-master:9000/user/isabel/3760.txt")

    val mm = MapMatching.run(sc, edges, nodes, traj)

    //val vertSeq = graph_.vertices.map(v => v._1).collect().toSeq
    //val sp = ShortestPaths.run(graph_, vertSeq)
    //sp.vertices.collect().foreach(println)
    //sp.vertices.saveAsObjectFile("hdfs://ldiag-master:9000/user/isabel/sp")

    //val spp = sc.objectFile[(VertexId, ShortestPaths.SPMap)]("hdfs://ldiag-master:9000/user/isabel/sp")
    //spp.collect().foreach(println)

    val elapsedTime = System.currentTimeMillis() - startTime
    println("Time: " + elapsedTime + " ms")
  }
}