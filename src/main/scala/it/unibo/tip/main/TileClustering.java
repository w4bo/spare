package it.unibo.tip.main;

import apriori.MainApp;
import cluster.*;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import model.Point;
import model.SimpleCluster;
import model.SnapShot;
import model.SnapshotClusters;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class TileClustering implements ClusteringMethod {
    private static final Logger logger = Logger.getLogger(TileClustering.class);
    private final int M;

    public class TileWrapper implements Function<Tuple2<Integer, SnapShot>, SnapshotClusters> {

        @Override
        public SnapshotClusters call(Tuple2<Integer, SnapShot> v1) throws Exception {
            final long time_start = System.currentTimeMillis();
            final int timestamp = v1._1;
            final Map<Pair<Integer, Integer>, Set<Integer>> clusters = Maps.newLinkedHashMap();
            for (int pid: v1._2.getObjects()) {
                final Point p = v1._2.getPoint(pid);
                final Integer lat = (int) p.getLat();
                final Integer lon = (int) p.getLon();
                clusters.compute(Pair.of(lat, lon), (key, value) -> {
                    Set<Integer> res = value;
                    if (res == null) res = Sets.newLinkedHashSet();
                    res.add(pid);
                    return res;
                });
            }

            SnapshotClusters result = new SnapshotClusters(v1._1);
            for (Map.Entry<Pair<Integer, Integer>, Set<Integer>> cluster : clusters.entrySet()) {
                if (cluster.getValue().size() >= M) {
                    SimpleCluster sc = new SimpleCluster();
                    sc.addObjects(cluster.getValue());
                    sc.setID(UUID.randomUUID().toString());
                    result.addCluster(sc);
                    logger.info("Timestamp: " + timestamp + ", c: " + sc);
                }
            }
            long time_end = System.currentTimeMillis();
            logger.info("Timestamp: " + timestamp + ", Objects: " + v1._2.getObjects().size() + ", Clusters:  " + result.getClusterSize() + ", Time (ms): " + (time_end - time_start));
            return result;
        }
    }

    private static final SnapshotGenerator ssg = new SnapshotGenerator();
    private static final SnapshotCombinor ssc = new SnapshotCombinor();
    private static final TupleFilter tf = new TupleFilter();

    private TileWrapper dwr;
    private int pars;

    public TileClustering(int m, int partitions) {
        this.M = m;
        this.pars = partitions;
        dwr = new TileWrapper();
    }

    @Override
    public JavaRDD<SnapshotClusters> doClustering(JavaRDD<String> input) {
        JavaPairRDD<Integer, SnapShot> ts_clusters = input.filter(tf).mapToPair(ssg).reduceByKey(ssc, pars);
        // Key is the time sequence
        return ts_clusters.map(dwr);
    }
}
