import it.unibo.tip.main.ClusteringAndSPAREMain
import it.unimi.dsi.fastutil.ints.IntRBTreeSet
import it.unimi.dsi.fastutil.ints.IntSet
import it.unimi.dsi.fastutil.ints.IntSortedSet
import java.io.BufferedReader
import java.io.FileReader
import java.io.IOException
import java.util
import org.junit.Test
import scala.Tuple2


class TestMain {
    @Test def test01(): Unit = {
        ClusteringAndSPAREMain.execute("src/main/resources/SimpleDataset.tsv", "src/main/resources/output", 1, 1, 1, 1, 1, 1)
    }
}
