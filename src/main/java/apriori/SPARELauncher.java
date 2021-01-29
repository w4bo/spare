package apriori;

import it.unimi.dsi.fastutil.ints.IntSet;
import model.SnapshotClusters;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

    private String hdfsInputPath;
    private String hdfsOutputPath;
    //Number of the input partition
    private int input_partition = 10;
    private int gcmpM;
    private int gcmpK;
    private int gcmpL;
    private int gcmpG;

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
     * Default constructor for the class.
     *
     * @param hdfsInputPath  the input path
     * @param hdfsOutputPath the output path
     * @param gcmpM          m parameter for GCMP
     * @param gcmpK          k parameter for GCMP
     * @param gcmpL          l parameter for GCMP
     * @param gcmpG          g parameter for GCMP
     */
    public SPARELauncher(String hdfsInputPath, String hdfsOutputPath, int gcmpM, int gcmpK, int gcmpL, int gcmpG) throws Exception {
        this(hdfsInputPath, hdfsOutputPath, gcmpM, gcmpK, gcmpL, gcmpG, 10);
    }

    /**
     * Launch and execute the SPARE algorithm.
     */
    public JavaRDD<IntSet> executeSpare(final JavaSparkContext context) throws IOException {
        // .set("spark.executor.instances", "4")
        // .set("spark.executor.cores", "5");
        // JavaSparkContext context = new JavaSparkContext(conf);
        //Load input data directly from HDFS and split into the desired number of cluster.
        JavaRDD<SnapshotClusters> CLUSTERS = context.objectFile(this.hdfsInputPath, this.input_partition);

        if (CLUSTERS.collect().size() == 0) {
            System.out.println("No cluster is found, skipping...");
            return null;
        }

        AlgoLayout al = new AprioriLayout(this.gcmpK, this.gcmpM, this.gcmpL, this.gcmpG, this.input_partition);
        al.setInput(CLUSTERS);
//	//starting apriori: build a star and fragment its edges
        JavaRDD<IntSet> output = al.runLogic().filter(
                new Function<IntSet, Boolean>() {
                    private static final long serialVersionUID = 1854327010963412841L;

                    @Override
                    public Boolean call(IntSet v1) throws Exception {
                        return v1.size() > 0;
                    }
                });
        this.checkOutputFolder(context, hdfsOutputPath);
        System.out.println("############### Results!!!");
        System.out.println(hdfsOutputPath);
        List<IntSet> grounds = output.collect();
        output.filter(new DuplicateClusterFilter(grounds)).saveAsTextFile(hdfsOutputPath);
        output.filter(new DuplicateClusterFilter(grounds)).collect().forEach(System.out::println);
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
