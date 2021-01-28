package apriori;

import java.io.Serializable;
import it.unimi.dsi.fastutil.ints.IntSet;
import model.SnapshotClusters;

import org.apache.spark.api.java.JavaRDD;

/**
 * Define an interface for an APriori algorithm.
 */
public interface AlgoLayout extends Serializable {
    /**
     * Set up the input inside the algorithm.
     * @param CLUSTERS an RDD of Snapshot cluster.
     */
    void setInput(JavaRDD<SnapshotClusters> CLUSTERS);

    /**
     * Run the algorithm.
     * @return a set of object ids that have travelled together.
     */
    JavaRDD<IntSet> runLogic();
}
