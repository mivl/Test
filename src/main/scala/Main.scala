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

  def main(arg: Array[String]) {
    val startTime = System.currentTimeMillis()

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val jobName = "MapMatching"

    val conf = new SparkConf().setAppName(jobName).setMaster("spark://ldiag-master:7077").
      set("spark.executor.memory", "6g").set("spark.driver.memory", "3g")

    val sc = new SparkContext(conf)

    sc.addJar("/home/isabel/IdeaProjects/Test/target/scala-2.10/Test-assembly-1.0.jar")

    val edges = sc.textFile("hdfs://ldiag-master:9000/user/isabel/fortaleza/graph_edges.txt", 200)
    val nodes = sc.textFile("hdfs://ldiag-master:9000/user/isabel/fortaleza/graph_nodes.txt", 200)
    val traj = sc.textFile("hdfs://ldiag-master:9000/user/isabel/650_ex.txt", 200)

    val mm = MapMatching.run(sc, edges, nodes, traj)

    val elapsedTime = System.currentTimeMillis() - startTime
    println("Time: " + elapsedTime + " ms")
  }
}