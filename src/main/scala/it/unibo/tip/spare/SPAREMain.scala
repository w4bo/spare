//package it.unibo.tip.spare
//
//import apriori.SPARELauncher
//import it.unibo.tip.timer.Timer
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.rogach.scallop.ScallopConf
//
//object SPAREMain extends App {
//    val conf = new Conf(args)
//    val name = "Apriori-K" + conf.gcmp_k() + "-L" + conf.gcmp_l() + "-M" + conf.gcmp_m() + "-G" + conf.gcmp_g() + "-File" + conf.input_dir()
//    if (conf.debug.supplied) {
//        if (conf.debug() equals "OFF") {
//            Logger.getLogger("org").setLevel(Level.OFF);
//            Logger.getLogger("aka").setLevel(Level.OFF);
//        }
//    }
//
//
//    /*This is set according to what is written on the paper*/
//    val sparkConf = new SparkConf().setAppName(name)
//    if (conf.executormemory.supplied) sparkConf.set("spark.executor.memory", conf.executormemory())
//    if (conf.numexecutors.supplied) sparkConf.set("spark.executor.instances", conf.numexecutors().toString)
//    if (conf.numcores.supplied) sparkConf.set("spark.executor.cores", conf.numcores().toString)
//    val timer = Timer()
//    if (conf.input_partitions.supplied) {
//        val spareLauncher = new SPARELauncher(conf.input_dir(), conf.output_dir(), conf.gcmp_m(), conf.gcmp_k(), conf.gcmp_l(), conf.gcmp_g(), conf.input_partitions())
//        spareLauncher executeSpare (sparkConf)
//    } else {
//        new SPARELauncher(conf.input_dir(), conf.output_dir(), conf.gcmp_m(), conf.gcmp_k(), conf.gcmp_l(), conf.gcmp_g()) executeSpare (sparkConf)
//    }
//    println(s"Time elapsed ${timer.getTimeInSeconds()} seconds")
//}
//
///**
// * Class to be used to parse CLI commands, the values declared inside specify name and type of the arguments to parse.
// *
// * @param arguments the programs arguments as an array of strings.
// */
//class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
//    val input_dir = opt[String](required = true)
//    val output_dir = opt[String](required = true)
//    val gcmp_m = opt[Int](required = true)
//    val gcmp_k = opt[Int](required = true)
//    val gcmp_l = opt[Int](required = true)
//    val gcmp_g = opt[Int](required = true)
//    val input_partitions = opt[Int]()
//    val numexecutors = opt[Int]()
//    val numcores = opt[Int]()
//    val executormemory = opt[String]()
//    val debug = opt[String]()
//    verify()
//}
