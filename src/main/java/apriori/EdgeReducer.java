package apriori;

import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import org.apache.spark.api.java.function.Function2;

/**
 * A proper reudcible function for combining edges with the same t
 * This is mandatory to build GA: it builds tuples like: ((i,j):[t1,t2,...,tn])
 * where (i,j) is the key and [t1,t2,..., tn] the sorted set of instants.
 *
 * @author a0048267
 */
public class EdgeReducer implements Function2<IntSortedSet, IntSortedSet, IntSortedSet> {
    private static final long serialVersionUID = -522176775845102773L;

    @Override
    public IntSortedSet call(IntSortedSet v1, IntSortedSet v2) throws Exception {
        final IntRBTreeSet set = new IntRBTreeSet();
        set.addAll(v1);
        set.addAll(v2);
        return set;
    }
}
