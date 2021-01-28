package apriori;

import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import java.util.ArrayList;
import java.util.Iterator;

import model.SimpleCluster;
import model.SnapshotClusters;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 * This is the first operation applied to the snapshots
 * For each snapshots t, define all the tuples ((i, j), t) where
 * i < j and i,j are Objects(or trajectories) ID and t is the timestamp of the current snaphot.
 * The output of this phase is the G(t) Graph foreach t saved as a iterable of snapshots.
 * @author a0048267
 * 
 */
public class EdgeSegmentor
	implements
	PairFlatMapFunction<SnapshotClusters, Tuple2<Integer, Integer>, IntSortedSet> {
    /**
     * 
     */
    private static final long serialVersionUID = 4125348116998762164L;

    @Override
    public Iterator<Tuple2<Tuple2<Integer, Integer>, IntSortedSet>> call(final SnapshotClusters t) throws Exception {

	ArrayList<Tuple2<Tuple2<Integer, Integer>, IntSortedSet>> results = new ArrayList<>();
	int my_ts = t.getTimeStamp();
	IntSortedSet current = new IntRBTreeSet();
	current.add(my_ts);

	//For each cluster inside the set...
	for (SimpleCluster sc : t.getClusters()) {
	    // each cluster generates {n \choose 2} Integer-Integer pairs
		// i create all the possible edges(a edge is created when two objects appears inside the same cluster)
	    IntSet objectset = sc.getObjects();
	    // change from iterable to random accessible
	    int[] cluster = objectset.toArray(new int[objectset.size()]);
	    // pair-wise join to create edge segment
	    for (int i = 0; i < cluster.length; i++) {
		for (int j = i + 1; j < cluster.length; j++) {
		    // ensures that outer is always smaller than inner
		    int outer = cluster[i];
		    int inner = cluster[j];
		    if (outer > inner) {
			int tmp = outer;
			outer = inner;
			inner = tmp;
		    }
		    Tuple2<Tuple2<Integer, Integer>, IntSortedSet> segment = new Tuple2<>(new Tuple2<>(outer, inner), current);
		    results.add(segment);
		}
	    }
	}
	return results.iterator();
    }

}
