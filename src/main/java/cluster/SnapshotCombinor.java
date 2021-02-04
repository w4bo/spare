package cluster;

import model.SnapShot;
import org.apache.spark.api.java.function.Function2;

/**
 * Another functional wrapper for merging two snapshots together.
 */
public class SnapshotCombinor implements Function2<SnapShot, SnapShot, SnapShot> {
    private static final long serialVersionUID = -3440630456333492120L;

    @Override
    public SnapShot call(SnapShot v1, SnapShot v2) throws Exception {
        SnapShot res = new SnapShot(v1.getTS());
        res.MergeWith(v1);
        res.MergeWith(v2);
        return res;
        // v1.MergeWith(v2);
        // return v1;
    }
}
