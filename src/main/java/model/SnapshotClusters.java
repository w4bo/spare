package model;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.Serializable;

/**
 * The class contains a set of SimpleCluster of at the 
 * same time sequence. ObjectArrayList from fastutil library is used to
 * boost the performance.
 * Output of the clustering process.
 * 
 * @author a0048267
 *
 */
public class SnapshotClusters implements Serializable {
    private static final long serialVersionUID = 9162568949845610013L;
    private int ts;
//    private ArrayList<SimpleCluster> clusters;
    private ObjectArrayList<SimpleCluster> clusters;
    
    /**
     * create an empty snapshot with a time sequence
     * @param time
     */
    public SnapshotClusters(int time){
	ts = time;
//	clusters = new ArrayList<>();
	clusters = new ObjectArrayList<>();
    }
    
    public Iterable<SimpleCluster> getClusters() {
	return clusters;
    }
    
    public SimpleCluster getClusterAt(int index) {
	return clusters.get(index);
    }
    
    public int getTimeStamp() {
	return ts;
    }
    
    public int getClusterSize() {
	return clusters.size();
    }
    
    public void addCluster(SimpleCluster sc) {
	clusters.add(sc);
    }
    
    @Override
    public String toString() {
	return "<"+ ts+ ":"+ clusters+ ">";
    }
}
