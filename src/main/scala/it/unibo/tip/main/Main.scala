package it.unibo.tip.main

import apriori.SPARELauncher
import input.SnapshotGenerator
import it.unibo.tip.timer.Timer
import it.unimi.dsi.fastutil.ints.IntSet
import org.apache.commons.io.FilenameUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}
import java.util

class Main {}

/**
 * This class will include the clustering and spare process together
 */
object Main {
  private val logger = Logger.getLogger(classOf[Main])

  def execute(input: String, outputdir: String, m: Int, k: Int, l: Int, g: Int, eps: Int = -1, minpts: Int = -1, exec: Int = 1, ram: String = "1g", cores: Int = 1, earth: Int = 1, master: String = "yarn", inputTable: String = ""): util.List[IntSet] = {
    val config = s"${FilenameUtils.getBaseName(input)}-K_$k-L_$l-M_$m-G_$g"
    val clusterDir = outputdir + config + "/clusters"
    val itemsetDir = outputdir + config + "/itemsets"
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("aka").setLevel(Level.OFF);
    val spark =
      SparkSession.builder()
        .appName("Spare-" + config)
        .master(master)
        .config("spark.master", master)
        .config("spark.executor.memory", ram)
        .config("spark.executor.instances", exec)
        .config("spark.executor.cores", cores)
        .enableHiveSupport()
        .getOrCreate()
    logger.info("Begin")
    val partitions = exec * cores * 3
    val timer = Timer() // init the timer
    val jsc = new JavaSparkContext(spark.sparkContext) // create the spark context
    val snapshotGenerator = new SnapshotGenerator(eps, minpts, input, clusterDir, partitions, m, earth)
    val runOnCluster = !master.startsWith("local")

    val clusters =
      if (inputTable.nonEmpty) {
        snapshotGenerator.cluster(
          spark
            .sql(s"select itemid, latitude, longitude, time_bucket from $inputTable")
            .rdd
            .map(row => s"${row.get(0)}\t${row.get(1)}\t${row.get(2)}\t${row.get(3)}")
            .toJavaRDD, jsc, runOnCluster)
      } else {
        snapshotGenerator.cluster(jsc, runOnCluster) // create the clusters (save to hdfs if not running on local)
      }

    logger.info("Done clustering")
    val spareLauncher = new SPARELauncher(clusterDir, itemsetDir, m, k, l, g, partitions)
    val output = spareLauncher executeSpare(jsc, clusters)
    val timeName = s"logs/time_" + config
    writeTimeOnFile(timeName, timer.getTimeInMillis())
    logger.info("done")
    if (!runOnCluster) output.collect() else null
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    execute(
      conf.input.getOrElse(""),
      conf.output(),
      conf.m(),
      conf.k(),
      conf.l(),
      conf.g(),
      conf.eps.getOrElse(-1),
      conf.minpts.getOrElse(-1),
      conf.executors(),
      conf.ram(),
      conf.cores(),
      conf.earth.getOrElse(-1),
      conf.master.getOrElse("yarn"),
      conf.inputtable.getOrElse("")
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
  val input = opt[String]()
  val inputtable = opt[String]()
  val output = opt[String](required = true)
  val m = opt[Int](required = true)
  val k = opt[Int](required = true)
  val l = opt[Int](required = true)
  val g = opt[Int](required = true)
  val executors = opt[Int]()
  val cores = opt[Int]()
  val ram = opt[String]()
  val eps = opt[Int]()
  val minpts = opt[Int]()
  val earth = opt[Int]()
  val master = opt[String]()
  verify()
}
