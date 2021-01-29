import it.unibo.tip.main.ClusteringAndSPAREMain
import it.unimi.dsi.fastutil.ints.IntSet
import org.junit.Test


class TestMain {
    @Test def test01(): Unit = {
        val res = ClusteringAndSPAREMain
            .execute("src/main/resources/SimpleDataset.tsv", "src/main/resources/output", 1, 1, 1, 1, 1, 1)
            .collect()
            .toArray()
        res.foreach(i => println(i))
    }
}
