package apriori;

import java.util.List;

import it.unimi.dsi.fastutil.ints.IntSet;
import model.SnapshotClusters;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import conf.AppProperties;

/**
 * the entry point of apriori method.
 * Aprioir method utilizes the object partition for 
 * parallelizing trajectory pattern mining, which
 * would support SWARM patterns.
 * @author a0048267
 *
 */
public class MainApp {
	private static final Logger logger = Logger.getLogger(MainApp.class);
    public static void main(String[] args) {
	//load with default values, and pass-in values from here
	//the conf.constants should not be used at now
	int K = 40, L = 10, M = 10, G = 3;
	int clique_miner_partitions = 486;
	
	if(args.length > 0) {
	    for(String arg : args) {
		logger.debug(arg);
		if(arg.startsWith("k=") || arg.startsWith("K=")) {
		    K = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("l=")|| arg.startsWith("L=")) {
		    L = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("m=")|| arg.startsWith("M=")) {
		    M = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("g=")|| arg.startsWith("G=")) {
		    G = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("c=")|| arg.startsWith("C=")) {
		    clique_miner_partitions = Integer.parseInt(arg.split("=")[1]);
		} 
	    }
	} else {
	    logger.debug("No commandline arguments found. Using default values instead");
	    logger.debug("Usage: .bin/spark-submit --class apriori.MainApp ~/TrajectoryMining/TrajectoryMining-0.0.1-SNAPSHOT-jar-with-dependencies.jar " +
	    		"k=40 l=10 g=3 h=195 e=15 p=10 c=115 s=115");
	    logger.debug("Missing values are replaced by defaults!");
	    System.exit(-1);
	}
	
	String hdfs_input = "/user/fnaldini/GCMPGeoLife/snapshots/clusters-e5-p10/";
	String name = "Apriori-K" + K + "-L" + L + "-M" + M + "-G" + G + "-File"+hdfs_input;
	//Logger.getLogger("org").setLevel(Level.OFF);
	//Logger.getLogger("aka").setLevel(Level.OFF);
	
	SparkConf conf = new SparkConf().setAppName(name)
			.set("spark.executor.instances", "55")
		    .set("spark.executor.cores", "2")
			.set("spark.executor.memory", "5g");
	       // .set("spark.driver.memory","20g");
	JavaSparkContext context = new JavaSparkContext(conf);
	//Load input data directly from HDFS and split into the desired number of cluster.
	JavaRDD<SnapshotClusters> CLUSTERS = context.objectFile(hdfs_input, clique_miner_partitions);
	
	AlgoLayout al = new AprioriLayout(K, M, L, G, clique_miner_partitions);
//	AlgoLayout al = new AprioriWithLB(K, M, L, G, clique_miner_partitions);
	al.setInput(CLUSTERS);
//	//starting apriori: build a star and fragment its edges
	JavaRDD<IntSet> output = al.runLogic().filter(
		new Function<IntSet,Boolean>(){
		    private static final long serialVersionUID = 1854327010963412841L;
		    @Override
		    public Boolean call(IntSet v1) throws Exception {
			return v1.size() > 0;
		    }
		}); // we do not need distinct here, since the star-based partition guranteed distinctness
	//need further removal of duplicates
	//if a pattern (1,2,3,4) is found, it may like found (2,3,4) from other machine
	List<IntSet> grounds = output.collect();
	List<IntSet> duplicate_removed = output.filter(new DuplicateClusterFilter(grounds)).collect();
	for(IntSet each_output : duplicate_removed) {
	    logger.debug(each_output);
	}
	context.close();
    }
}
