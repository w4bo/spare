package cluster;

import java.io.Serializable;
import model.SnapshotClusters;
import org.apache.spark.api.java.JavaRDD;

/**
 * Interface for the Clustering method, could be useful somewhere.
 */
public interface ClusteringMethod extends Serializable{
    JavaRDD<SnapshotClusters> doClustering(JavaRDD<String> input);
}
