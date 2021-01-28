package apriori;

import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import model.SnapshotClusters;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Map;

import static org.apache.spark.api.java.StorageLevels.DISK_ONLY;
import static org.apache.spark.api.java.StorageLevels.MEMORY_AND_DISK;

/**
 * Implementation of the AlgoLayout of Apriori.
 * This phase goes to generate the GA graph and generate the tuples.
 */
public class AprioriLayout implements AlgoLayout {
    private static final long serialVersionUID = 1013697935052286484L;
    private JavaRDD<SnapshotClusters> input;
    private int K, L, M, G;
    private int clique_partitions;
    
   
    private EdgeSegmentor edge_seg;
    private EdgeReducer edge_reducer;
    private EdgeFilter edge_filter;
    private EdgeMapper edge_mapper;
    private CliqueMiner clique_miner;
    private EdgeLSimplification edge_simplifier;
    
    
    public AprioriLayout(int k, int m, int l, int g) {
	K = k; 
	L = l;
	M = m;
	G = g;
	edge_seg = new EdgeSegmentor();
	edge_reducer = new EdgeReducer();
	edge_filter = new EdgeFilter(K,M,L,G);
	edge_mapper = new EdgeMapper();
	clique_miner = new CliqueMiner(K,M,L,G);
//	clique_miner = new EagerCliqueMiner(K,M,L,G); 
	edge_simplifier = new EdgeLSimplification(K, L, G);
	clique_partitions = 195; //32 executors
    }
    
    public AprioriLayout(int k, int m, int l, int g, int pars) {
 	K = k; 
 	L = l;
 	M = m;
 	G = g;
 	edge_seg = new EdgeSegmentor();
 	edge_reducer = new EdgeReducer();
 	edge_filter = new EdgeFilter(K,M,L,G);
 	edge_mapper = new EdgeMapper();
 	clique_miner = new CliqueMiner(K,M,L,G);
// 	clique_miner = new EagerCliqueMiner(K,M,L,G); 
 	edge_simplifier = new EdgeLSimplification(K, L, G);
 	clique_partitions = pars;
     }
    
    @Override
    public void setInput(JavaRDD<SnapshotClusters> CLUSTERS) {
	input = CLUSTERS;
    }

    @Override
    public JavaRDD<IntSet> runLogic() {
	
	// Create G(t) for each snapshot t
	JavaPairRDD<Tuple2<Integer, Integer>, IntSortedSet> stage1 = input.flatMapToPair(edge_seg)
		.persist(MEMORY_AND_DISK);
	System.out.println("Sum of all the edges from every G(t) inside the system:\t" + stage1.count());
	
	/*
	Build the GA of the whole dataset, merging intsets by key, so merge together two edges if they connect the
	same two nodes(objects or trajectories, does not matter), so builds tuples in the form
	*/
	JavaPairRDD<Tuple2<Integer, Integer>, IntSortedSet> stage2 = stage1.reduceByKey(edge_reducer);
//		.cache();
	System.out.println("Condensed edges in connection graph:\t"+stage2.count());
	
	JavaPairRDD<Tuple2<Integer, Integer>, IntSortedSet> stage3 = stage2
		.mapValues(edge_simplifier) //L-semplification
		.filter(edge_filter) //G-semplification
		//.cache(); //This should be modified
		.persist(MEMORY_AND_DISK);
	System.out.println("2-long itemset with semplified time-sequence:\t" + stage3.count());
	
	//For each ObjectID(or TrajectoryID), create and group tuples on the key(TrajectoryID)
		// The output is in the form of (i, Array[(j, [t1,..,tn])])
	JavaPairRDD<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> stage4 
		= stage3.mapToPair(edge_mapper)
		.groupByKey(clique_partitions)
		.persist(DISK_ONLY);
	Map<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> stage4result = stage4.collectAsMap();
	System.out.println("Star size distribution:");
	for(Map.Entry<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> entry : stage4result.entrySet()) {
	    Iterator<Tuple2<Integer,IntSortedSet>> itr = entry.getValue().iterator();
	    int count = 0;
	    while(itr.hasNext()) {
		count++;
		itr.next();
	    }
	    System.out.println(entry.getKey() + "\t" + count);
	}
	//Apriori mining for each star
	//Change to store to cache
	JavaRDD<IntSet> stage5 = 
		stage4.flatMap(clique_miner)
				.persist(MEMORY_AND_DISK);
	return stage5;
    }

}
