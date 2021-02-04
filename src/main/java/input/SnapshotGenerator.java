package input;

import cluster.ClusteringMethod;
import it.unibo.tip.main.TileClustering;
import model.SnapshotClusters;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;


/**
 * This class is a simple wrapper for the generation of
 * the snapshots from the clusters
 */
public class SnapshotGenerator {

    private int epsilon;
    private int minPoints;
    private String hdfsInputPath;
    private String hdfsOutputPath;
    //Number of the input partition
    private int hdfs_partition = 10;
    /*Number of the output partition*/
    private int snapshot_partitions = 10;
    private int gcmpM;
    int earth = 1;

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
     * @return
     */
    public JavaRDD<SnapshotClusters> cluster(final JavaSparkContext context, final int bins, final boolean savehdfs) throws IOException {
        System.out.println("Epsilon is " + epsilon);
        System.out.println("M is " + gcmpM);
        // JavaSparkContext context = new JavaSparkContext(conf);
        //JavaRDD<String> input = context.textFile(this.hdfsInputPath, hdfs_partitions);
        JavaRDD<String> input = context.textFile(this.hdfsInputPath);
        input = removeTSVHeader(input);
        // ClusteringMethod cm = new BasicClustering(this.epsilon, this.minPoints, this.gcmpM, snapshot_partitions, earth);
        ClusteringMethod cm = new TileClustering(bins, bins, this.gcmpM, snapshot_partitions);
        JavaRDD<SnapshotClusters> CLUSTERS = cm.doClustering(input);
        if (savehdfs) {
            CLUSTERS.collect().forEach(System.out::println);
            String hdfs_out = String.format(this.hdfsOutputPath + "/clusters-e%d-p%d", this.epsilon, this.minPoints);
            this.checkOutputFolder(context, hdfsOutputPath);
            CLUSTERS.saveAsObjectFile(hdfs_out);
        }
        return CLUSTERS;
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
        System.out.println("Header is " + header);
        return inputRDD.filter(row -> !row.equals(header));
    }
}
