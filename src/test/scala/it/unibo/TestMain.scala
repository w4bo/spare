package it.unibo

import it.unibo.tip.main.Main
import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalatest.junit.JUnitSuite

import scala.collection.JavaConversions.collectionAsScalaIterable


class TestMain extends JUnitSuite {
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
        val res: Set[Set[Int]] = Main.execute(dataset, s"$path/output", m, k, l, g, master = "local[1]").map(is => is.toIntArray.toSet).toSet
        assertEquals(expected, res)
    }

    @Test def testFlock(): Unit = {
        check(s"$path/flock.tsv", Set(Set(1, 2)), 2, 2, 2, 1)
    }

    @Test def testFlock1(): Unit = {
        check(s"$path/flock1.tsv", Set(Set(1, 2)), 2, 2, 2, 1)
    }

    @Test def testFlock2(): Unit = {
        check(s"$path/flock2.tsv", Set(Set(1, 2)), 2, 3, 3, 1)
        check(s"$path/flock2.tsv", Set(), 3, 3, 3, 1)
    }

    @Test def testFlockFail(): Unit = {
        check(s"$path/flockfail.tsv", Set(), 2, 4, 4, 1)
    }

    @Test def testSwarm(): Unit = {
        check(s"$path/swarm.tsv", Set(Set(1, 2, 3)), 3, 2, 1, Integer.MAX_VALUE)
    }

//    @Test def testPlatoon(): Unit = {
//        check(s"$path/platoon.tsv", Set(Set(1, 2)), 4, 2, 2, Integer.MAX_VALUE)
//    }
}
