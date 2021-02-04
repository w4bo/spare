import it.unibo.tip.main.ClusteringAndSPAREMain
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.JavaConversions.collectionAsScalaIterable


class TestMain {
    val path = "src/main/resources"

    /**
     * @param dataset  dataset
     * @param expected result
     * @param m        min moving objects
     * @param k        min length
     * @param l        min length of consecutive subsequence
     * @param g        gap between subsequences
     */
    def check(dataset: String, expected: Set[Set[Int]], m: Int, k: Int, l: Int, g: Int): Unit = {
        val res: Set[Set[Int]] = ClusteringAndSPAREMain.execute(dataset, s"$path/output", m, k, l, g).map(is => is.toIntArray.toSet).toSet
        assertEquals(expected, res)
    }

    @Test def test01(): Unit = {
        val res = ClusteringAndSPAREMain.execute(s"$path/SimpleDataset.tsv", s"$path/output", 1, 1, 1, 1)
        res.toArray.foreach(i => println(i))
    }

    @Test def testFlock(): Unit = {
        check(s"$path/flock.tsv", Set(Set(1, 2)), 2, 2, 2, 1)
        check(s"$path/flock1.tsv", Set(Set(11, 12)), 2, 2, 2, 1)
        check(s"$path/flock2.tsv", Set(Set(1, 2)), 2, 2, 2, 1)
    }

    @Test def testSwarm(): Unit = {
        check(s"$path/flock.tsv", Set(Set(1, 2)), 2, 2, 2, 1)
        check(s"$path/flock1.tsv", Set(Set(11, 12)), 2, 2, 2, 1)
        check(s"$path/flock2.tsv", Set(Set(1, 1)), 2, 2, 2, 1)
    }
}
