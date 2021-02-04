package it.unibo.tip.main

import apriori.{DuplicateClusterFilter, SPARELauncher}
import input.SnapshotGenerator
import it.unibo.tip.timer.Timer
import it.unimi.dsi.fastutil.ints.IntSet
import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}
import java.util
import java.util.List

class ClusteringAndSPAREMain {}

/**
 * This class will include the clustering and spare process together
 */
object ClusteringAndSPAREMain {
  private val logger = Logger.getLogger(classOf[TileClustering])

  def execute(dataset: String, outputdir: String,
              m: Int, k: Int, l: Int, g: Int, bins: Int = 10,
              eps: Int = -1, minpts: Int = -1,
              exec: Int = 1, ram: String = "1g", cores: Int = 1, part: Int = 10, earth: Int = 1, master: String = "local[1]"): util.List[IntSet] = {
    val clusterOutputPath = outputdir + "/clusters/"
    val snapshotInputPath = s"$clusterOutputPath/clusters-e${eps}-p${minpts}"
    val spareName = "Apriori-K" + k + "-L" + l + "-M" + m + "-G" + g + "-File" + snapshotInputPath
    logger.info(spareName)
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("aka").setLevel(Level.OFF);
    val spark =
      SparkSession.builder()
        .appName(spareName)
        .master(master)
        .config("spark.master", master)
        .config("spark.executor.memory", ram)
        .config("spark.executor.instances", exec + "")
        .config("spark.executor.cores", cores + "")
        .getOrCreate()
    val jsc = new JavaSparkContext(spark.sparkContext)
    val timer = Timer()
    val snapshotGenerator = new SnapshotGenerator(eps, minpts, dataset, clusterOutputPath, part, m, earth)
    val clusters = snapshotGenerator.cluster(jsc, bins, false)
    val spareLauncher = new SPARELauncher(snapshotInputPath, outputdir, m, k, l, g, part)
    val output = spareLauncher executeSpare(jsc, clusters)
    logger.debug(s"Elapsed seconds: ${timer.getTimeInSeconds()}")
    val timeName = s"logs/time_" + outputdir.replace("/", "_")
    writeTimeOnFile(timeName, if (output != null) timer.getTimeInMillis() else Long.MaxValue)
    if (output != null) output.collect() else null
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    execute(
      conf.input_dir(),
      conf.output_dir(),
      conf.gcmp_m(),
      conf.gcmp_k(),
      conf.gcmp_l(),
      conf.gcmp_g(),
      conf.bins(),
      conf.epsilon(),
      conf.min_points(),
      conf.numexecutors(),
      conf.executormemory(),
      conf.numcores(),
      conf.input_partitions.getOrElse(10),
      conf.earth.getOrElse(1)
    )
  }

  def writeTimeOnFile(fileTimeName: String, timeToWrite: Long): Unit = {
    val timeFileExists = Files.exists(Paths.get(fileTimeName))
    val timeOutputFile = new File(fileTimeName)
    if (timeFileExists) {
      timeOutputFile.delete()
    }
    timeOutputFile.createNewFile()
    val bw = new BufferedWriter(new FileWriter(fileTimeName))
    bw.write(s"${timeToWrite}")
    bw.close()
  }

}

/**
 * Class to be used to parse CLI commands, the values declared inside specify name and type of the arguments to parse.
 *
 * @param arguments the programs arguments as an array of strings.
 */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input_dir = opt[String](required = true)
  val output_dir = opt[String](required = true)
  val gcmp_m = opt[Int](required = true)
  val gcmp_k = opt[Int](required = true)
  val gcmp_l = opt[Int](required = true)
  val gcmp_g = opt[Int](required = true)
  val input_partitions = opt[Int]()
  val bins = opt[Int]()
  val numexecutors = opt[Int]()
  val numcores = opt[Int]()
  val executormemory = opt[String]()
  val debug = opt[String]()
  val epsilon = opt[Int](required = true)
  val min_points = opt[Int](required = true)
  val earth = opt[Int]()
  verify()
}
