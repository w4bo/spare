package it.unibo

import it.unibo.tip.main.Main
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

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
        val res: Set[Set[Int]] = Main.execute(dataset, s"$path/output", m, k, l, g, master = "local[1]").map(is => collection.immutable.SortedSet[Int]() ++ is.toIntArray.toSet).toSet
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

    @Test def testOldenburg(): Unit = {
        check(s"$path/join__oldenburg_standard_2000_distinct__4__5__20__absolute__1.tsv", Set(Set(15, 24, 21, 47), Set(16, 17, 3, 23), Set(32, 10, 4, 47), Set(19, 1, 29, 23)), 4, 5, 1, Integer.MAX_VALUE)
        check(s"$path/tmp_transactiontable__tbl_oldenburg_standard_2000_distinct__lmt_50__size_4__sup_5__bins_20__ts_absolute__bint_1.tsv", Set(Set(15, 24, 21, 47), Set(16, 17, 3, 23), Set(32, 10, 4, 47), Set(19, 1, 29, 23)), 4, 5, 1, Integer.MAX_VALUE)
    }

    @Test def testAllFrequent(): Unit = {
        check(s"$path/all_frequent.tsv", Set(Set(1, 2, 3)), 3, 10, 1, Integer.MAX_VALUE)
    }

    @Test def testOldenburg1(): Unit = {
        check(s"$path/join__oldenburg_standard_2000_distinct__10__35__20__absolute__1_red5.tsv", Set(Set(981, 805, 324)), 2, 5, 1, Integer.MAX_VALUE)
        check(s"$path/join__oldenburg_standard_2000_distinct__10__35__20__absolute__1_red.tsv", Set(Set(981, 805, 324)), 2, 35, 1, Integer.MAX_VALUE)
    }

    @Test def testOldenburg2(): Unit = {
        var output = ""

        output = filterfile(s"$path/join__oldenburg_standard_2000_distinct__10__34__20__absolute__1.tsv", Set(524, 915, 1046), Set((41, 41), (46, 47)))
        check(output, Set(Set(524, 915, 1046)), 2, 1, 1, Integer.MAX_VALUE)

        output = filterfile(s"$path/join__oldenburg_standard_2000_distinct__10__34__20__absolute__1.tsv", Set(524, 915, 1046), Set((40, 52)))
        check(output, Set(Set(524, 915, 1046)), 2, 4, 1, Integer.MAX_VALUE)

        output = filterfile(s"$path/join__oldenburg_standard_2000_distinct__10__34__20__absolute__1.tsv", Set(524, 915, 1046), Set((43, 44), (47, 47)))
        check(output, Set(Set(524, 915), Set(524, 1046)), 2, 2, 1, Integer.MAX_VALUE)

        output = filterfile(s"$path/join__oldenburg_standard_2000_distinct__10__34__20__absolute__1.tsv", Set(524, 915, 1046), Set((42, 44), (47, 47)))
        check(output, Set(Set(524, 915), Set(524, 1046)), 2, 3, 1, Integer.MAX_VALUE)

         output = filterfile(s"$path/join__oldenburg_standard_2000_distinct__10__34__20__absolute__1.tsv", Set(524, 915, 1046), Set((3, 52)))
         check(output, Set(Set(524, 915, 1046)), 2, 27, 1, Integer.MAX_VALUE)
    }

    def filterfile(filename: String, ids: Set[Int], timestamps: Set[(Int, Int)] = Set()): String = {
        val source = scala.io.Source.fromFile(filename)
        val lines: String = try source.mkString finally source.close()
        val content = lines.split("\n").map(i => i.split("\t")).filter(i =>
            i(0).contains("id") ||
            ids.contains(i(0).toInt) &&
            (timestamps.isEmpty || timestamps.exists(t => t._1 <= i(3).toInt && i(3).toInt <= t._2))
        ).map(i => i.reduce(_ + "\t" + _)).reduce(_ + "\n" + _)
        import java.io.PrintWriter
        val output = filename.replace(".tsv", "") + "_" + ids.hashCode() + "_" + timestamps.hashCode() + "__tmp.tsv"
        new PrintWriter(output) {
            write(content); close()
        }
        output
    }

    //    @Test def testPlatoon(): Unit = {
    //        check(s"$path/platoon.tsv", Set(Set(1, 2)), 4, 2, 2, Integer.MAX_VALUE)
    //    }
}
