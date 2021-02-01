import com.google.common.collect.Sets
import it.unibo.tip.main.ClusteringAndSPAREMain
import it.unimi.dsi.fastutil.ints.{IntCollection, IntIterator, IntSet}
import org.junit.Assert.assertEquals
import org.junit.Test

import java.util


class TestMain {
    val path = "src/main/resources"
    @Test def test01(): Unit = {
        val res = ClusteringAndSPAREMain.execute(s"$path/SimpleDataset.tsv", s"$path/output", 1, 1, 1, 1).collect().toArray()
        res.foreach(i => println(i))
    }

    @Test def testFlock(): Unit = {
        val res = ClusteringAndSPAREMain.execute(s"$path/flock1.tsv", s"$path/output", 2, 1, 2, 1).collect().toArray()
        assertEquals(Array(Sets.newHashSet(1, 2)), res.asInstanceOf[Array[IntSet]])
    }
}
