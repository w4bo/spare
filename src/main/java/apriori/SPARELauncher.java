package apriori;

import it.unimi.dsi.fastutil.ints.IntSet;
import model.SnapshotClusters;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * This class is just a wrapper for the MainApp code, to improve its launch
 */
public class SPARELauncher implements Serializable {
    private static final Logger logger = Logger.getLogger(SPARELauncher.class);
    private final String hdfsInputPath;
    private final String hdfsOutputPath;
    private final int input_partition;
    private final int gcmpM;
    private final int gcmpK;
    private final int gcmpL;
    private final int gcmpG;

    /**
     * Default constructor for the class.
     *
     * @param hdfsInputPath   the input path
     * @param hdfsOutputPath  the output path
     * @param gcmpM           m parameter for GCMP
     * @param gcmpK           k parameter for GCMP
     * @param gcmpL           l parameter for GCMP
     * @param gcmpG           g parameter for GCMP
     * @param input_partition the number of partition in which the input will be split.
     */
    public SPARELauncher(String hdfsInputPath, String hdfsOutputPath, int gcmpM, int gcmpK, int gcmpL, int gcmpG, int input_partition) {
        this.hdfsInputPath = hdfsInputPath;
        this.hdfsOutputPath = hdfsOutputPath;
        this.gcmpM = gcmpM;
        this.gcmpK = gcmpK;
        this.gcmpL = gcmpL;
        this.gcmpG = gcmpG;
        this.input_partition = input_partition;
    }

    /**
     * Launch and execute the SPARE algorithm.
     */
    public JavaRDD<IntSet> executeSpare(final JavaSparkContext context, final JavaRDD<SnapshotClusters> prevClusters) throws IOException {
        // .set("spark.executor.instances", "4")
        // .set("spark.executor.cores", "5");
        // JavaSparkContext context = new JavaSparkContext(conf);
        // Load input data directly from HDFS and split into the desired number of cluster.
        final JavaRDD<SnapshotClusters> clusters = prevClusters == null ? context.objectFile(this.hdfsInputPath, this.input_partition) : prevClusters;
        final AlgoLayout al = new AprioriLayout(this.gcmpK, this.gcmpM, this.gcmpL, this.gcmpG, this.input_partition);
        al.setInput(clusters);
        final JavaRDD<IntSet> output = al.runLogic().filter(v1 -> v1.size() > 0);
        checkOutputFolder(context, hdfsOutputPath);
        final List<IntSet> grounds = output.collect();
        if (prevClusters == null) {
            output.filter(new DuplicateClusterFilter(grounds)).saveAsTextFile(hdfsOutputPath);
            return null;
        } else {
            return output.filter(new DuplicateClusterFilter(grounds));
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
}
