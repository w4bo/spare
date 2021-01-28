//package it.unibo.tip.snapshotgen
//
//import input.SnapshotGenerator
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.rogach.scallop.ScallopConf
//
//object SnapshotGenerationMain extends App {
//    val conf = new Conf(args)
//    var snapshotPartition = 10
//    val name = "DBSCAN-E=" + conf.epsilon() + "-P=" + conf.minPoints()
//    if (conf.debug.supplied) {
//        if (conf.debug() equals "OFF") {
//            Logger.getLogger("org").setLevel(Level.OFF);
//            Logger.getLogger("aka").setLevel(Level.OFF);
//        }
//    }
//
//    val sparkConf = new SparkConf().setAppName(name)
//    if (conf.executormemory.supplied) sparkConf.set("spark.executor.memory", conf.executormemory())
//    if (conf.numexecutors.supplied) sparkConf.set("spark.executor.instances", conf.numexecutors().toString)
//    if (conf.numcores.supplied) sparkConf.set("spark.executor.cores", conf.numcores().toString)
//    if (conf.snapshotPartition.supplied) snapshotPartition = conf.snapshotPartition()
//    val snapshotGenerator = new SnapshotGenerator(conf.epsilon(), conf.minPoints(), conf.input_file(), conf.output_dir(), snapshotPartition, conf.gcmpm())
//    snapshotGenerator.cluster(sparkConf)
//}
//
///**
// * Class to be used to parse CLI commands, the values declared inside specify name and type of the arguments to parse.
// *
// * @param arguments the programs arguments as an array of strings.
// */
//class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
//    val input_file = opt[String](required = true)
//    val output_dir = opt[String](required = true)
//    val epsilon = opt[Int](required = true)
//    val minPoints = opt[Int](required = true)
//    val snapshotPartition = opt[Int]()
//    val numexecutors = opt[Int]()
//    val numcores = opt[Int]()
//    val executormemory = opt[String]()
//    val gcmpm = opt[Int](required = true)
//    val debug = opt[String]()
//    val earth = opt[Int]()
//    verify()
//}
