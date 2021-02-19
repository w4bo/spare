package apriori;

import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import model.SnapshotClusters;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import static org.apache.spark.api.java.StorageLevels.MEMORY_AND_DISK;

/**
 * Implementation of the AlgoLayout of Apriori.
 * This phase goes to generate the GA graph and generate the tuples.
 */
public class AprioriLayout implements AlgoLayout {
    private static final Logger logger = Logger.getLogger(AprioriLayout.class);
    private static final long serialVersionUID = 1013697935052286484L;
    private JavaRDD<SnapshotClusters> input;
    private final int K, L, M, G, partitions;
    private final EdgeSegmentor edge_seg;
    private final EdgeReducer edge_reducer;
    private final EdgeFilter edge_filter;
    private final EdgeMapper edge_mapper;
    private final CliqueMiner clique_miner;
    // private final EagerCliqueMiner clique_miner;
    private final EdgeLSimplification edge_simplifier;

    public AprioriLayout(int k, int m, int l, int g, int pars) {
        this(k, m, l, g, pars, null);
    }

    public AprioriLayout(int k, int m, int l, int g, int pars, final LongAccumulator acc) {
        K = k;
        L = l;
        M = m;
        G = g;
        edge_seg = new EdgeSegmentor();
        edge_reducer = new EdgeReducer();
        edge_filter = new EdgeFilter(K, M, L, G);
        edge_mapper = new EdgeMapper();
        clique_miner = new CliqueMiner(K, M, L, G, acc);
        // clique_miner = new EagerCliqueMiner(K,M,L,G);
        edge_simplifier = new EdgeLSimplification(K, L, G);
        partitions = pars;
    }

    @Override
    public void setInput(JavaRDD<SnapshotClusters> clusters) {
        input = clusters;
    }

    @Override
    public JavaRDD<IntSet> runLogic() {
        // Create G(t) for each snapshot t
        final JavaPairRDD<Tuple2<Integer, Integer>, IntSortedSet> stage1 = input.flatMapToPair(edge_seg); // .persist(MEMORY_AND_DISK)
        // stage1.persist(MEMORY_AND_DISK).collect().forEach(System.out::println);
        // System.out.println("----");
        // logger.debug("Sum of all the edges from every G(t) inside the system: " + stage1.count());

        // Build the GA of the whole dataset, merging intsets by key, so merge together two edges if they connect the
        // same two nodes(objects or trajectories, does not matter), so builds tuples in the form
        final JavaPairRDD<Tuple2<Integer, Integer>, IntSortedSet> stage2 = stage1.reduceByKey(edge_reducer, partitions); // .persist(MEMORY_AND_DISK)
        // stage2.persist(MEMORY_AND_DISK).collect().forEach(System.out::println);
        // System.out.println("----");
        // logger.debug("Condensed edges in connection graph:\t" + stage2.count());

        final JavaPairRDD<Tuple2<Integer, Integer>, IntSortedSet> stage3 = stage2
                .mapValues(edge_simplifier) // L-semplification
                .filter(edge_filter); // G-semplification
        //.cache(); //This should be modified
        // .persist(MEMORY_AND_DISK);
        // logger.debug("2-long itemset with semplified time-sequence:\t" + stage3.count());
        // stage3.persist(MEMORY_AND_DISK).collect().forEach(System.out::println);
        // System.out.println("----");

        // For each ObjectID(or TrajectoryID), create and group tuples on the key(TrajectoryID)
        // The output is in the form of (i, Array[(j, [t1,..,tn])])
        final JavaPairRDD<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> stage4 = stage3.mapToPair(edge_mapper).groupByKey(partitions);
        // stage4.persist(MEMORY_AND_DISK).collect().forEach(System.out::println);
        // System.out.println("----");

        // begin my comment
        // final JavaPairRDD<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> stage4 = stage3.mapToPair(edge_mapper).groupByKey(partitions).persist(MEMORY_AND_DISK);
        // final Map<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> stage4result = stage4.collectAsMap();
        // logger.debug("Star size distribution:");
        // for (Map.Entry<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> entry : stage4result.entrySet()) {
        //     Iterator<Tuple2<Integer, IntSortedSet>> itr = entry.getValue().iterator();
        //     int count = 0;
        //     while (itr.hasNext()) {
        //         count++;
        //         itr.next();
        //     }
        //     logger.debug(entry.getKey() + "\t" + count);
        // }
        // end my comment

        // Apriori mining for each star
        // Change to store to cache
        return stage4.flatMap(clique_miner).persist(MEMORY_AND_DISK); //
    }
}
