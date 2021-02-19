package apriori;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Mining cliques from a star-like structure.
 * This does the Apriori Enumeration as a function inside a flatmap
 *
 * @author a0048267
 */
public class CliqueMiner implements FlatMapFunction<Tuple2<Integer, Iterable<Tuple2<Integer, IntSortedSet>>>, IntSet> {
    private static final Logger logger = Logger.getLogger(CliqueMiner.class);
    private static final long serialVersionUID = 714635813712741661L;

    // L and G are used for later prunings
    private final int K;
    private final int M;
    private final int G;
    private final EdgeLSimplification simplifier;
    private final LongAccumulator acc;

    public CliqueMiner(int k, int m, int l, int g) {
        this(k, m, l, g, null);
    }

    public CliqueMiner(int k, int m, int l, int g, final LongAccumulator acc) {
        K = k;
        M = m;
        G = g;
        simplifier = new EdgeLSimplification(K, l, G);
        this.acc = acc;
    }

    private void printCliquerDebug(final String message, final int vertex) {
        logger.debug("CLIQUER_DEBUG[" + vertex + "]: " + message);
    }

    @Override
    public Iterator<IntSet> call(Tuple2<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> starVertex) throws Exception {
        Iterator<Tuple2<Integer, IntSortedSet>> starEdges = starVertex._2.iterator();
        int count = 0;
        while (starEdges.hasNext()) {
            count++;
            starEdges.next();
//	     Tuple2<Integer, IntSortedSet> tuple = tmp.next();
////	     print out input for debugging purpose
//	     logger.debug(tuple._1 + "\t" + tuple._2);
        }
        printCliquerDebug("Edges for " + starVertex._1 + " is " + count, starVertex._1);
        // time anchors to record running time
        long t_start, t_end;
        // use Apriori method for mining the cliques in bottom-up manner
        // each edge represents a 2-frequent itemset
        // building higher frequent itemset iteratively
        HashMap<IntSet, IntSortedSet> timestamp_store = new HashMap<>(); // contains all the tuples in the form of (i,j,[t1,..,tn])
        ArrayList<IntSet> ground = new ArrayList<>(); // contains all the itemset(i,j)
        ArrayList<IntSet> output = new ArrayList<>();
        ArrayList<IntSet> candidate;
        // initialization: regenerate the tuples in the form of (i,j,[t1,..,tn]) and add them to timestamp and ground
        for (Tuple2<Integer, IntSortedSet> startEdge : starVertex._2) {
            IntSet cluster = new IntOpenHashSet();
            cluster.add(startEdge._1);
            cluster.add(starVertex._1);
            timestamp_store.put(cluster, startEdge._2);
            ground.add(cluster);
        }
        candidate = ground;
        int level = 1;

        //Here Apriori pruning happens
        while (true) {
            t_start = System.currentTimeMillis();
            ArrayList<IntSet> nextLevel = new ArrayList<>();
            HashSet<IntSet> duplicates = new HashSet<>(); // do not add duplicate objectset to the next level
            for (final IntSet candidateItemset : candidate) {
                acc.add(1L);
                boolean existValidSuperSet = false;
                // for each candidate, generate all the possible combination tuples
                for (final IntSet groundItemset : ground) {
                    acc.add(1L);
                    if (candidateItemset.containsAll(groundItemset)) {
                        // a candidate should not join with its subset;
                        continue;
                    }
                    printCliquerDebug("Trying to merge " + candidateItemset + " with " + groundItemset, starVertex._1);
                    // Create new candidate itemset
                    IntSet newCandidateItemset = new IntOpenHashSet();
                    newCandidateItemset.addAll(groundItemset);
                    newCandidateItemset.addAll(candidateItemset);
                    // Intersect all the timestamp between each itemset couple
                    final IntSortedSet intersectedTimestampSet = new IntRBTreeSet();
                    intersectedTimestampSet.addAll(timestamp_store.get(groundItemset));
                    intersectedTimestampSet.retainAll(timestamp_store.get(candidateItemset));

                    // execute F-G simplification on the new intersected sequence.
                    final IntSortedSet simplifiedIntersectedTimestampSet = simplifier.call(intersectedTimestampSet);

                    if (simplifiedIntersectedTimestampSet.size() >= K) {
                        // the new candidate is significant and is potential and it has not already been generated.
                        printCliquerDebug("Itemset " + newCandidateItemset + " is potentially viable for K=" + K, starVertex._1);
                        if (!duplicates.contains(newCandidateItemset)) {
                            nextLevel.add(newCandidateItemset);
                            duplicates.add(newCandidateItemset);
                            timestamp_store.put(newCandidateItemset, simplifiedIntersectedTimestampSet);
                        }
                        // then this candidate is not qualified for
                        // output
                        existValidSuperSet = true;
                    }
                }
                /*
                 * pruned is not actually pruning, in simple terms, if there's at least a potential valid superset of
                 * the current itemset, the itemset is not tested in output, otherwise check if the current itemset is a valid candidate
                 */
                if (!existValidSuperSet) {
                    IntSortedSet time_stamps = timestamp_store.get(candidateItemset);
                    time_stamps = simplifier.call(time_stamps);
                    if (time_stamps.size() >= K) {
                        // time_stamp is greater than K
                        if (candidateItemset.size() >= M) {
                            output.add(candidateItemset);
                        }
                    }
                }
            }
            t_end = System.currentTimeMillis();
            long time_taken = t_end - t_start;
            printCliquerDebug("Object-Grow: " + level + ", " + time_taken + " ms  cand_size:" + candidate.size(), starVertex._1);

            level++;
            if (nextLevel.isEmpty()) {
                printCliquerDebug("Exit on level " + level, starVertex._1);
                break;
            } else {
                candidate = nextLevel;
                // forward closure testing
                if (candidate.size() != 0) {

                    IntSet eagerset = new IntOpenHashSet();
                    IntSortedSet eagerstamps = new IntRBTreeSet();
                    //Get the first itemset and its time sequence.
                    eagerset.addAll(candidate.get(0));
                    eagerstamps.addAll(timestamp_store.get(candidate.get(0)));

                    boolean early_flag = false;

                    for (int i = 1; i < candidate.size(); i++) {
                        IntSet cand = candidate.get(i);
                        eagerset.addAll(candidate.get(i));
                        eagerstamps.retainAll(timestamp_store.get(cand));
                        if (eagerstamps.size() < K) {
                            early_flag = true; // early terminates the join if
                            // timestmaps already reduces
                            // less than K
                            break;
                        }
                    }
                    if (!early_flag) {
                        if (eagerset.size() < M) { // no patterns with size M
                            // can be found
                            printCliquerDebug("Closure check directly terminates.", starVertex._1);
                            break;
                        }
                        eagerstamps = simplifier.call(eagerstamps);
                        if (eagerstamps.size() >= K) {
                            if (eagerset.size() >= M) {
                                candidate.clear();
                                candidate.add(eagerset);
                                timestamp_store.put(eagerset, eagerstamps);
                                output.add(eagerset); //POTENTIAL FIX FOR THE OLD SPARE, BUT CAN CAUSE POTENTIAL PROBLEMS HERE
                                printCliquerDebug("Closure check finished from level " + (level - 1) + " to " + (eagerset.size() - 2), starVertex._1);
                                level = eagerset.size() - 2;
                                break;
                            }
                        }
                    }
                }
            }
        }
        printCliquerDebug("Finished", starVertex._1);
        return output.iterator();
    }

}
