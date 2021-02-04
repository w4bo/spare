package cluster;

import apriori.MainApp;
import model.SimpleCluster;
import model.SnapShot;
import model.SnapshotClusters;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import util.DBSCANClustering;

import java.util.ArrayList;

/**
 * this class was written by the devil itself.
 * It's a sort of wrapper to make a function to wrap the DBSCAN algorithm i suppose because of Spark
 * functional implementation and the inability of the guy who wrote this mess to understand Scala.
 */
public class DBSCANWrapper implements Function<Tuple2<Integer, SnapShot>, SnapshotClusters> {
    private static final Logger logger = Logger.getLogger(MainApp.class);
    private static final long serialVersionUID = 3562163124094087749L;
    private int eps, minPts;
    private int M;
    private int earth;

    public DBSCANWrapper(int ieps, int iminPts, int m, int earth) {
        eps = ieps;
        minPts = iminPts;
        M = m;
        this.earth = earth;
    }

    @Override
    public SnapshotClusters call(Tuple2<Integer, SnapShot> v1) throws Exception {
        long time_start = System.currentTimeMillis();
        DBSCANClustering dbc = new DBSCANClustering(eps, minPts, v1._2, earth);
        ArrayList<SimpleCluster> clusters = dbc.cluster();
        SnapshotClusters result = new SnapshotClusters(v1._1);
        for (SimpleCluster cluster : clusters) {
            if (cluster.getObjects().size() >= M) {
                SimpleCluster sc = new SimpleCluster();
                sc.addObjects(cluster.getObjects());
                sc.setID(cluster.getID());
                result.addCluster(sc);
            }
        }
        long time_end = System.currentTimeMillis();
        // remove when actual deploy
        logger.debug("Objects to be clustered: "
                + v1._2.getObjects().size() + "  " + (time_end - time_start)
                + " ms" + "\t" + result.getClusterSize());
        return result;
    }
}
