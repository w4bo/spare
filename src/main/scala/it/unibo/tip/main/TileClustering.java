package it.unibo.tip.main;

import cluster.*;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import model.Point;
import model.SimpleCluster;
import model.SnapShot;
import model.SnapshotClusters;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Map;
import java.util.Set;

public class TileClustering implements ClusteringMethod {

    private final int M;
    private final int bin_lat;
    private final int bin_lon;

    public class TileWrapper implements Function<Tuple2<Integer, SnapShot>, SnapshotClusters> {

        @Override
        public SnapshotClusters call(Tuple2<Integer, SnapShot> v1) throws Exception {
            final long time_start = System.currentTimeMillis();

            final Map<Pair<Double, Double>, Set<Integer>> clusters = Maps.newLinkedHashMap();
            for (int pid: v1._2.getObjects()) {
                final Point p = v1._2.getPoint(pid);
                final double lat = Math.round(p.getLat() / (11 * bin_lat)) * (11 * bin_lat);
                final double lon = Math.round(p.getLon() / (15 * bin_lon)) * (15 * bin_lon);
                clusters.compute(Pair.of(lat, lon), (key, value) -> {
                    Set<Integer> res = value;
                    if (res == null) res = Sets.newHashSet(pid);
                    else res.add(pid);
                    return res;
                });
            }

            int idx = 1;
            SnapshotClusters result = new SnapshotClusters(v1._1);
            for (Map.Entry<Pair<Double, Double>, Set<Integer>> cluster : clusters.entrySet()) {
                if (cluster.getValue().size() >= M) {
                    SimpleCluster sc = new SimpleCluster();
                    sc.addObjects(cluster.getValue());
                    sc.setID("c" + idx++);
                    result.addCluster(sc);
                }
            }
            long time_end = System.currentTimeMillis();
            System.out.println("Objects to be clustered: " + v1._2.getObjects().size() + "  " + (time_end - time_start) + " ms" + "\t" + result.getClusterSize());
            return result;
        }
    }

    private static final SnapshotGenerator ssg = new SnapshotGenerator();
    private static final SnapshotCombinor ssc = new SnapshotCombinor();
    private static final TupleFilter tf = new TupleFilter();

    private TileWrapper dwr;
    private int pars;

    public TileClustering(int bin_lat, int bin_lon, int M, int pars) {
        this.pars = pars;
        this.bin_lat = bin_lat;
        this.bin_lon = bin_lon;
        this.M = M;
        dwr = new TileWrapper();
    }

    @Override
    public JavaRDD<SnapshotClusters> doClustering(JavaRDD<String> input) {
        JavaPairRDD<Integer, SnapShot> TS_CLUSTERS = input.filter(tf).mapToPair(ssg).reduceByKey(ssc, pars);
        // Key is the time sequence
        JavaRDD<SnapshotClusters> CLUSTERS = TS_CLUSTERS.map(dwr);
        return CLUSTERS;
    }
}
