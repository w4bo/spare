package input;

import apriori.MainApp;
import cluster.ClusteringMethod;
import it.unibo.tip.main.TileClustering;
import model.SnapshotClusters;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;


/**
 * This class is a simple wrapper for the generation of
 * the snapshots from the clusters
 */
public class SnapshotGenerator {
    private static final Logger logger = Logger.getLogger(MainApp.class);
    private final int epsilon;
    private final int minPoints;
    private final String hdfsInputPath;
    private final String hdfsOutputPath;
    private final int snapshot_partitions;
    private final int gcmpM;
    private final int earth;

    /**
     * Default constructor with all the parameters.
     *
     * @param epsilon             the epsilon value of dbscan.
     * @param minPoints           the min points value of dbscan.
     * @param hdfsInputPath       the input path of the files.
     * @param hdfsOutputPath      the output path of the files.
     * @param snapshot_partitions the number of required output partition.
     * @param gcmpM               M parameter for the GCMP algorithm.
     * @param earth               a flag for selecting if the distance between points is euclidean or geographic.
     */
    public SnapshotGenerator(int epsilon, int minPoints, String hdfsInputPath, String hdfsOutputPath, int snapshot_partitions, int gcmpM, int earth) {
        this.epsilon = epsilon;
        this.minPoints = minPoints;
        this.hdfsInputPath = hdfsInputPath;
        this.hdfsOutputPath = hdfsOutputPath;
        this.snapshot_partitions = snapshot_partitions;
        this.gcmpM = gcmpM;
        this.earth = earth;
    }

    /**
     * Execute the same operation of the MainApp class
     *
     * @return
     */
    public JavaRDD<SnapshotClusters> cluster(final JavaSparkContext context, final boolean runOnCluster) throws IOException {
        // JavaSparkContext context = new JavaSparkContext(conf);
        // JavaRDD<String> input = context.textFile(hdfsInputPath, hdfs_partitions);
        final JavaRDD<String> input = removeTSVHeader(context.textFile(hdfsInputPath));
        // ClusteringMethod cm = new BasicClustering(epsilon, minPoints, gcmpM, snapshot_partitions, earth);
        final ClusteringMethod cm = new TileClustering(gcmpM, snapshot_partitions);
        final JavaRDD<SnapshotClusters> clusters = cm.doClustering(input);
        if (runOnCluster) {
            // final String hdfs_out = String.format(hdfsOutputPath + "/clusters", epsilon, minPoints);
            final String hdfs_out = hdfsOutputPath + "/clusters";
            checkOutputFolder(context, hdfsOutputPath);
            clusters.saveAsObjectFile(hdfs_out);
            return null;
        } else {
            return clusters;
        }
    }

    /**
     * Check if the output folder is present, and if so it delete it
     */
    private void checkOutputFolder(final JavaSparkContext sparkContext, final String hdfsOutputPath) throws IOException {
        final FileSystem fileSystem = FileSystem.get(sparkContext.hadoopConfiguration());
        final Path outputPath = new Path(hdfsOutputPath);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
    }

    private JavaRDD<String> removeTSVHeader(final JavaRDD<String> inputRDD) {
        String header = inputRDD.first();
        logger.debug("Header is " + header);
        return inputRDD.filter(row -> !row.equals(header));
    }
}
