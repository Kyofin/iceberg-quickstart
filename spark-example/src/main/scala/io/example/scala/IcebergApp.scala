package io.example.scala

import org.apache.spark.sql.SparkSession

object IcebergApp {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local")
      .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type","hadoop")
      .config("spark.sql.catalog.local.warehouse","file:///Volumes/Samsung_T5/opensource/iceberg-quickstart/iceberg_warehouse")
      .getOrCreate()

    import spark.implicits._

    val df = List(
      (1, "pp", 22,"20210201"),
      (3, "pp", 22,"20210202"),
      (4, "pp", 22,"20210201"),
      (5, "pp", 22,"20210201"),
      (2, "pp", 32,"20210202")
    ).toDF("id", "name", "age","dt")

    spark.sql(
      """
        |CREATE TABLE if not EXISTS local.db.tb1 (
        |id bigint COMMENT 'user id',
        |name string ,
        |age int ,
        |dt string)
        | USING iceberg
        |PARTITIONED BY (dt)
        |TBLPROPERTIES(
        | 'format-version'	='2'
        |)
        |
        |""".stripMargin)

    df.writeTo("local.db.tb1").append()

//    df.write.format("iceberg").mode("overwrite").saveAsTable("local.db.tb1")

    spark.sql("delete from  local.db.tb1 where id =1")
    spark.read.format("iceberg").table("local.db.tb1").show()

    spark.stop()
  }

}
