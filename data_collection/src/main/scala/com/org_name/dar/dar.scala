package com.org_name.dar

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import com.databricks.spark.avro._

object Main {
    def main(args: Array[String]) {
        val inputDir = args(0)
        val outputDir = args(1)
        val conf = new SparkConf()
        val sc = new SparkContext(conf)
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)

        val rawApiData = sqlContext.read.format("com.databricks.spark.avro").load(inputDir)
        rawApiData.registerTempTable("rawApiData")

        val dealAudData = sqlContext.sql("select aId,bId,cId,dId,sum(e) as sume,sum(f) as sumf,sum(g) as sumg,count(*) as impr from rawApiData where cId != 0 and dId is not null group by aId,bId,cId,dId")
        val dealAudData2 = dealAudData.coalesce(5)
        dealAudData2.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", "false").save(outputDir)

        sc.stop()
        }
}
