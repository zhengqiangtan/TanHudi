package com.hudi.test

import org.apache.spark.sql.SparkSession

object HudiQueryTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[2]")
      .appName(HudiQueryTest.getClass.getCanonicalName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate


    val tableName = "tb01"
    val basePath = "file:///Users/tandemac/workspace/binlog/sync_hudi/data/hudi_data/" + tableName

    val roViewDF = spark.
      read.
      format("org.apache.hudi").
      load(basePath + "/*/*/*/*")
    roViewDF.registerTempTable("hudi_ro_table")
    spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_ro_table where fare > 20.0").show(false)
    spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_ro_table").show(false)

    spark.stop()
  }

}

//+------------------+-------------------+-------------------+---+
//|              fare|          begin_lon|          begin_lat| ts|
//+------------------+-------------------+-------------------+---+
//| 27.79478688582596| 0.6273212202489661|0.11488393157088261|0.0|
//| 33.92216483948643| 0.9694586417848392| 0.1856488085068272|0.0|
//| 93.56018115236618|0.14285051259466197|0.21624150367601136|0.0|
//| 64.27696295884016| 0.4923479652912024| 0.5731835407930634|0.0|
//|34.158284716382845|0.46157858450465483| 0.4726905879569653|0.0|
//|  43.4923811219014| 0.8779402295427752| 0.6100070562136587|0.0|
//| 66.62084366450246|0.03844104444445928| 0.0750588760043035|0.0|
//| 41.06290929046368| 0.8192868687714224|  0.651058505660742|0.0|
//+------------------+-------------------+-------------------+---+




//+-------------------+------------------------------------+------------------------------------+---------+----------+------------------+
//|_hoodie_commit_time|_hoodie_record_key                  |_hoodie_partition_path              |rider    |driver    |fare              |
//+-------------------+------------------------------------+------------------------------------+---------+----------+------------------+
//|20200117114543     |bd720cdc-49c7-4471-bf22-8beed1bf2389|americas/united_states/san_francisco|rider-213|driver-213|27.79478688582596 |
//|20200117114543     |b73c498c-8b51-4e88-a6c3-0823cf862a09|americas/united_states/san_francisco|rider-213|driver-213|33.92216483948643 |
//|20200117114543     |c034358a-bc35-4124-b4e7-2c1fe4221543|americas/united_states/san_francisco|rider-213|driver-213|19.179139106643607|
//|20200117114543     |6d3903f6-6b32-4543-a334-3629cba3b9f7|americas/united_states/san_francisco|rider-213|driver-213|93.56018115236618 |
//|20200117114543     |9d8f3b3d-09ce-49a6-8597-c23acd92e82c|americas/united_states/san_francisco|rider-213|driver-213|64.27696295884016 |
//|20200117114543     |3ac04023-5e61-4244-894c-537e07cbd9be|americas/brazil/sao_paulo           |rider-213|driver-213|34.158284716382845|
//|20200117114543     |cf07ea8b-4f86-443b-8998-f91c17db4cd3|americas/brazil/sao_paulo           |rider-213|driver-213|43.4923811219014  |
//|20200117114543     |a7cf25aa-14b4-4fe0-b83e-4ee5cef1724e|americas/brazil/sao_paulo           |rider-213|driver-213|66.62084366450246 |
//|20200117114543     |8bc67e54-866d-41f3-a7fb-68489b9dd29e|asia/india/chennai                  |rider-213|driver-213|41.06290929046368 |
//|20200117114543     |396d212d-64d7-4e52-99be-a973ef306305|asia/india/chennai                  |rider-213|driver-213|17.851135255091155|
//+-------------------+------------------------------------+------------------------------------+---------+----------+------------------+