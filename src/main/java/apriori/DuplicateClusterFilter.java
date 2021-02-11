package apriori;

import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;

import java.util.Collection;

/**
 * Function to filter duplicate clusters (the patterns I suppose, but I'm not sure about it)
 */
public class DuplicateClusterFilter implements Function<IntSet, Boolean> {
    private static final Logger logger = Logger.getLogger(DuplicateClusterFilter.class);
    private static final long serialVersionUID = 8171848799948095816L;
    private final Collection<IntSet> grounds;

    public DuplicateClusterFilter(Collection<IntSet> ground) {
        grounds = ground;
    }

    @Override
    public Boolean call(IntSet v1) throws Exception {
        logger.debug("Ground:" + grounds + ", Value:" + v1);
        // remove duplicate intset
        for (IntSet ground : grounds) {
            if (ground.containsAll(v1) && ground.size() > v1.size()) {
                return false;
            }
        }
        return true;
    }
}
