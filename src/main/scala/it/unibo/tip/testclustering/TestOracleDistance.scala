package it.unibo.tip.testclustering

import model.{SimpleCluster, SnapShot, SnapshotClusters}
import util.DistanceOracle

object TestOracleDistance extends App {

  val basePoint = new model.Point(44.2230331,12.0375571)
  val inside1KMPoint = new model.Point(44.2280331, 12.0375571)
  val outside1KMPoint = new model.Point(44.2330331, 12.0375571)

  val otherlineOutside1KMPoint = new model.Point(44.2330331,12.0875571)
  val otherlineInside1KMPoint = new model.Point(44.2280331, 12.0875571)

  val snapshot = new SnapShot(0)
  snapshot.addObject(1, basePoint)
  snapshot.addObject(2, inside1KMPoint)
  snapshot.addObject(3, outside1KMPoint)
  snapshot.addObject(4, otherlineInside1KMPoint)
  snapshot.addObject(5, otherlineOutside1KMPoint)

  val scalaDBSCANClustering = new ScalaDBSCANClustering(600, 1, snapshot, 1)

  val outputclusters = scalaDBSCANClustering.cluster()

  println(outputclusters)

  val result = new SnapshotClusters(0)

  import scala.collection.JavaConversions._

  for (cluster <- outputclusters) {
    if (cluster.getObjects.size >= 3) {
      val sc = new SimpleCluster
      sc.addObjects(cluster.getObjects)
      sc.setID(cluster.getID)
      result.addCluster(sc)
    }
  }
  val time_end = System.currentTimeMillis
  // remove when actual deploy
  println(result)

  println("basePoint-inside1kmpoint:" + DistanceOracle.compEarthDistance(basePoint, inside1KMPoint))
  println("basePoint-outside1kmpoint:" + DistanceOracle.compEarthDistance(outside1KMPoint, basePoint))
  println("inside1kmpoint-outside1kmpoint:" + DistanceOracle.compEarthDistance(outside1KMPoint, inside1KMPoint))

  println("inside1kmpoint-otherlineInside1KMPoint:" + DistanceOracle.compEarthDistance(otherlineInside1KMPoint, inside1KMPoint))
  println("outside1kmpoint-otherlineOutside1KMPoint:" + DistanceOracle.compEarthDistance(otherlineOutside1KMPoint, outside1KMPoint))

  println("inside1kmpoint-otherlineOutside1KMPoint:" + DistanceOracle.compEarthDistance(otherlineOutside1KMPoint, inside1KMPoint))
  println("outside1kmpoint-otherlineInside1KMPoint:" + DistanceOracle.compEarthDistance(otherlineInside1KMPoint, outside1KMPoint))

  println("basepoint-otherlineInside1KMPoint:" + DistanceOracle.compEarthDistance(otherlineInside1KMPoint, basePoint))
  println("basepoint-otherlineOutside1KMPoint:" + DistanceOracle.compEarthDistance(otherlineOutside1KMPoint, basePoint))

  println("otherlineInside1KMPoint-otherlineOutside1KMPoint:" + DistanceOracle.compEarthDistance(otherlineInside1KMPoint, otherlineOutside1KMPoint))
}
