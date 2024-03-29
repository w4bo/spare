package apriori;

import it.unimi.dsi.fastutil.ints.IntSortedSet;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


/**
 * @author a0048267
 * Remap current tuples to
 * (i, (j,[t1,...,tn]))
 */
public class EdgeMapper implements PairFunction<Tuple2<Tuple2<Integer, Integer>, IntSortedSet>, Integer, Tuple2<Integer, IntSortedSet>> {
    private static final long serialVersionUID = 8125311113760199935L;

    @Override
    public Tuple2<Integer, Tuple2<Integer, IntSortedSet>> call(final Tuple2<Tuple2<Integer, Integer>, IntSortedSet> t) throws Exception {
        return new Tuple2<>(t._1._1, new Tuple2<>(t._1._2, t._2));
    }
}
