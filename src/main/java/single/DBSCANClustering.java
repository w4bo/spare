package single;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;  
import util.DistanceOracle;

import conf.AppProperties;


import model.Point;
import model.SimpleCluster;
import model.SnapShot;

/**
 * we use DBSCAN to cluster Points at the same snapshot
 * based on their closeness
 * @author a0048267
 *
 */
public class DBSCANClustering {
    // this counter is per JVM, i.e. per executor, thus, executor_id + ID_counter will
    // be a globally unique identifier for each cluster
    private static int ID_COUNTER = 0;
    /** Status of a point during the clustering process. */
    private enum PointStatus {
        /** The point has is considered to be noise. */
        NOISE,
        /** The point is already part of a cluster. */
        PART_OF_CLUSTER
    }
    private SnapShot sp;
    private ArrayList<SimpleCluster> clusters;
    private double eps;
    private double minPts;
    private int r;

    /**
     * Creates a new instance of a DBSCANClusterer.
     *
     * eps maximum radius of the neighborhood to be considered
     *  minPts minimum number of points needed for a cluster
     */
    public DBSCANClustering(SnapShot sp) {
//	this.eps = Double.parseDouble(AppProperties.getProperty("eps"));
//        this.minPts =Integer.parseInt(AppProperties.getProperty("minPts"));
	eps = Integer.parseInt(AppProperties.getProperty("eps"));
	minPts = Integer.parseInt(AppProperties.getProperty("minPts"));
        this.sp = sp;
        clusters = cluster();
    }
    
    public DBSCANClustering(double eps, int minPts, SnapShot sp) {
        this.eps = eps;
        this.minPts = minPts;
        this.sp = sp;
        clusters = cluster();
        r = 1;
    }
    
    public DBSCANClustering(double eps, int minPts, SnapShot sp, int r) {
        this.eps = eps;
        this.minPts = minPts;
        this.sp = sp;
        clusters = cluster();
        this.r =r;
    }
//    
    public ArrayList<SimpleCluster> getCluster() {
	return clusters;
    }

    /**
     * Performs DBSCAN cluster analysis.
     * <p>
     * <b>Note:</b> as DBSCAN is not a centroid-based clustering algorithm, the resulting
     * Cluster objects will have no defined center, i.e. Cluster.getCenter() will
     * return {@code null}.
     *
     *
     * @return the list of clusters
     */
    public ArrayList<SimpleCluster> cluster() {
//	ArrayList<Integer> noisies = new ArrayList<>();
        ArrayList<SimpleCluster> clusters = new ArrayList<>();
        Map<Integer, PointStatus> visited = new HashMap<>();
        for (Integer point : sp.getObjects()) {
            if (visited.get(point) != null) {
                continue;
            }
            ArrayList<Integer> neighbors = getNeighbors(point);
            if (neighbors.size() >= minPts) {
                // DBSCAN does not care about center points
        	SimpleCluster cluster = new SimpleCluster();
        	cluster.setID(ID_COUNTER+++"");
                clusters.add(expandCluster(cluster, point, neighbors, visited));
            } else {
                visited.put(point, PointStatus.NOISE);
            }
        }
//        clusters.add(noisies);
        return clusters;
    }

    /**
     * Expands the cluster to include density-reachable items.
     *
     * @param cluster Cluster to expand
     * @param point Point to add to cluster
     * @param neighbors List of neighbors
     * points the data set
     * @param visited the set of already visited points
     * @return the expanded cluster
     */
    private SimpleCluster expandCluster(SimpleCluster cluster,
                                     int point,
                                     ArrayList<Integer> neighbors,
                                     Map<Integer, PointStatus>  visited) {
        cluster.addObject(point);
        visited.put(point, PointStatus.PART_OF_CLUSTER);

        ArrayList<Integer> seeds = new ArrayList<Integer>(neighbors);
        int index = 0;
        while (index < seeds.size()) {
            int current = seeds.get(index);
            PointStatus pStatus = visited.get(current);
            // only check non-visited points
            if (pStatus == null) {
                ArrayList<Integer> currentNeighbors = getNeighbors(current);
                if (currentNeighbors.size() >= minPts) {
                    seeds = merge(seeds, currentNeighbors);
                }
            }

            if (pStatus != PointStatus.PART_OF_CLUSTER) {
                visited.put(current, PointStatus.PART_OF_CLUSTER);
                cluster.addObject(current);
            }
            index++;
        }
        return cluster;
    }

    /**
     * Returns a list of density-reachable neighbors of a {@code point}.
     *
     * @param point the point to look for
     * @points possible neighbors
     * @return the List of neighbors
     */
    private ArrayList<Integer> getNeighbors(int point) {
	ArrayList<Integer> neighbors = new ArrayList<Integer>();
        for (int neighbor : sp.getObjects()) {
            if (point != neighbor && dist(neighbor, point) <= eps) {
                neighbors.add(neighbor);
            }
        }
        return neighbors;
    }
    
    /**
     * compute distance between two points
     * @param neighbor
     * @param point
     * @return
     */
    private double dist(int neighbor, int point) {
	Point p1 = sp.getPoint(neighbor);
	Point p2 = sp.getPoint(point);
	if(r == 1) {
	    return DistanceOracle.compEarthDistance(p1, p2);
	} else {
	    return DistanceOracle.compEuclidianDistance(p1, p2);
	}
    }

    /**
     * Merges two lists together.
     *
     * @param one first list
     * @param two second list
     * @return merged lists
     */
    private ArrayList<Integer> merge(ArrayList<Integer> one, ArrayList<Integer> two) {
        HashSet<Integer> oneSet = new HashSet<Integer>(one);
        for (int item : two) {
            if (!oneSet.contains(item)) {
                one.add(item);
            }
        }
        return one;
    }
}
