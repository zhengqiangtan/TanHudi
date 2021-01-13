package com.hudi.test

import org.apache.hudi.QuickstartUtils.DataGenerator
import org.apache.spark.sql.SparkSession
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._


/**
 * 2020年1月17日 周五
 * hudi强制要求使用Kryo的序列化方式，所以初始化的时候需要添加该配置。
 *
 * https://hudi.apachecn.org/docs/0.5.0/quickstart.html
 *
 */
object HudiTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("Demo")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate


    val tableName = "hudi_trips_cow"
    val basePath = "file:///Users/tandemac/workspace/binlog/sync_hudi/data/hudi_data/" + tableName
    val dataGen = new DataGenerator()

    val inserts = convertToStringList(dataGen.generateInserts(10))
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, tableName).
      mode(Overwrite).
      save(basePath)


    spark.read.format("hudi").load(basePath + "/*/*/*/*").createOrReplaceTempView("hudi_trips_snapshot")
    spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 10.0").show()


    spark.stop()

  }
}