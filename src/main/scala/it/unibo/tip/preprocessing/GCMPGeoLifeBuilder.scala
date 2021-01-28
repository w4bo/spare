package it.unibo.tip.preprocessing

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * This object will sample the data inside the dataset according to the top N trajectories containing more than x points
 * order by numPoints asc.
 */
object GCMPGeoLifeBuilder extends App {
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    val FIELD_SEPARATOR = "\\t"
    val GEOLIFE_INPUT_PATH = "/user/fnaldini/GCMPGeoLife/dataset/dis_flated.dat"
    val GEOLIFE_OUTPUT_PATH = "/user/fnaldini/GCMPGeoLife/dataset/dis_flated_top200greater10/"
    val outputTableName = "filtered_dis_flated_GeoLife"
    val topNTableName = "top_n_table_name"
    val minNumPoints = 10
    val requiredTrajectories = 200

    val schema = new StructType()
        .add(StructField("TrajID", StringType))
        .add(StructField("Latitude", StringType))
        .add(StructField("Longitude", StringType))
        .add(StructField("Timestamp", StringType))

    spark.createDataFrame(sc.textFile(GEOLIFE_INPUT_PATH)
        .map(_.split(FIELD_SEPARATOR))
        .map(e => Row(e(0), e(1), e(2), e(3))), schema).createOrReplaceTempView(outputTableName)
    spark.sql(s"select TrajID, count(*) as points  from $outputTableName group by TrajID order by points asc").where(s"points > $minNumPoints").limit(requiredTrajectories).createOrReplaceTempView(topNTableName)
    spark.sql(s"select * from $outputTableName where TrajID in (select TrajID from $topNTableName)").write.mode("overwrite").option("sep", "\t").option("encoding", "UTF-8").csv(GEOLIFE_OUTPUT_PATH)
}
