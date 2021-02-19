package apriori;

import it.unimi.dsi.fastutil.ints.IntSet;
import model.SnapshotClusters;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * This class is just a wrapper for the MainApp code, to improve its launch
 */
public class SPARELauncher implements Serializable {
    private static final Logger logger = Logger.getLogger(SPARELauncher.class);
    private final String clusterDir;
    private final String itemsetDir;
    private final int partitions;
    private final int m;
    private final int k;
    private final int l;
    private final int g;
    private final LongAccumulator acc;

    /**
     * Default constructor for the class.
     *
     * @param clusterDir      the input path
     * @param itemsetDir      the output path
     * @param gcmpM           m parameter for GCMP
     * @param gcmpK           k parameter for GCMP
     * @param gcmpL           l parameter for GCMP
     * @param gcmpG           g parameter for GCMP
     * @param input_partition the number of partition in which the input will be split.
     */
    public SPARELauncher(String clusterDir, String itemsetDir, int gcmpM, int gcmpK, int gcmpL, int gcmpG, int input_partition, final LongAccumulator acc) {
        this.clusterDir = clusterDir;
        this.itemsetDir = itemsetDir;
        this.m = gcmpM;
        this.k = gcmpK;
        this.l = gcmpL;
        this.g = gcmpG;
        this.partitions = input_partition;
        this.acc = acc;
    }

    /**
     * Launch and execute the SPARE algorithm.
     */
    public JavaRDD<IntSet> executeSpare(final JavaSparkContext context, final JavaRDD<SnapshotClusters> prevClusters) throws IOException {
        // .set("spark.executor.instances", "4")
        // .set("spark.executor.cores", "5");
        // JavaSparkContext context = new JavaSparkContext(conf);
        // Load input data directly from HDFS and split into the desired number of cluster.
        final JavaRDD<SnapshotClusters> clusters = prevClusters == null ? context.objectFile(clusterDir, partitions) : prevClusters;
        final AlgoLayout al = new AprioriLayout(k, m, l, g, partitions, acc);
        al.setInput(clusters);
        JavaRDD<IntSet> output = al.runLogic().filter(v1 -> v1.size() > 0);
        checkOutputFolder(context, itemsetDir);
        final List<IntSet> grounds = output.collect();
        output = output.filter(new DuplicateClusterFilter(grounds)).cache();
        if (prevClusters == null) {
            output.repartition(1).saveAsTextFile(itemsetDir);
        }
        return output;
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
}
