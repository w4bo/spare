package it.unibo.tip.main

import apriori.SPARELauncher
import input.SnapshotGenerator
import it.unibo.tip.timer.Timer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}

/**
 * This class will include the clustering and spare process together
 */
object ClusteringAndSPAREMain {

    def execute(dataset: String, outputdir: String,
                k: Int, l: Int, m: Int, g: Int, eps: Int, minpts: Int,
                exec: Int = 1, ram: String = "1g", cores: Int = 1, part: Int = 10, earth: Int = 1, master: String = "local[1]"): Unit = {
        val clusterOutputPath = outputdir + "/clusters/"
        val snapshotInputPath = s"$clusterOutputPath/clusters-e${eps}-p${minpts}"
        val clusteringName = "DBSCAN-E=" + eps + "-P=" + minpts
        val spareName = "Apriori-K" + k + "-L" + l + "-M" + m + "-G" + g + "-File" + snapshotInputPath
        val spark =
            SparkSession.builder()
                .appName(clusteringName + " & " + spareName)
                .master(master)
                .config("spark.master", master)
                .config("spark.executor.memory", ram)
                .config("spark.executor.instances", exec + "")
                .config("spark.executor.cores", cores + "")
                .getOrCreate()
        val jsc = new JavaSparkContext(spark.sparkContext)
        val timer = Timer()
        val snapshotGenerator = new SnapshotGenerator(eps, minpts, dataset, clusterOutputPath, part, m, earth)
        snapshotGenerator.cluster(jsc)
        val spareLauncher = new SPARELauncher(snapshotInputPath, outputdir, m, k, l, g, part)
        val value = spareLauncher executeSpare (jsc)
        println(s"Elapsed seconds: ${timer.getTimeInSeconds()}")
        val timeName = s"logs/time_" + outputdir.replace("/", "_")
        if (value == 0) {
            writeTimeOnFile(timeName, timer.getTimeInMillis())
        } else {
            writeTimeOnFile(timeName, Long.MaxValue)
        }
    }

    def main(args: Array[String]): Unit = {
        val conf = new Conf(args)
        if (conf.debug.supplied) {
            if (conf.debug() equals "OFF") {
                Logger.getLogger("org").setLevel(Level.OFF);
                Logger.getLogger("aka").setLevel(Level.OFF);
            }
        }
        execute(
            conf.input_dir(),
            conf.output_dir(),
            conf.gcmp_k(),
            conf.gcmp_l(),
            conf.gcmp_m(),
            conf.gcmp_g(),
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
    val numexecutors = opt[Int]()
    val numcores = opt[Int]()
    val executormemory = opt[String]()
    val debug = opt[String]()
    val epsilon = opt[Int](required = true)
    val min_points = opt[Int](required = true)
    val earth = opt[Int]()
    verify()
}
