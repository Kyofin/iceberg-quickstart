package io.example.scala.warehouse.controller

import io.example.scala.warehouse.warehousePath
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DDLController {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .set("spark.sql.catalog.hadoop_prod.warehouse", s"$warehousePath")
      .set("spark.sql.catalog.catalog-name.type", "hadoop")
      .set("spark.sql.catalog.catalog-name.default-namespace", "default")
      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .set("spark.sql.session.timeZone", "GMT+8")
      .setMaster("local[*]")
      .setAppName("ddl_app")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    sparkSession.sql(
      """
        |create table hadoop_prod.db.dwd_member(
        |   uid int,
        |   ad_id int,
        |   birthday string,
        |   email string,
        |   fullname string,
        |   iconurl string,
        |   lastlogin string,
        |   mailaddr string,
        |   memberlevel string,
        |   password string,
        |   paymoney string,
        |   phone string,
        |   qq string,
        |   register string,
        |   regupdatetime string,
        |   unitname string,
        |   userip string,
        |   zipcode string,
        |   dt string)
        |  using iceberg
        |   partitioned by(dt);
        |
        |
        |
        |""".stripMargin)

    sparkSession.sql(
      """
        |create table hadoop_prod.db.dwd_member_regtype(
        |  uid int,
        |  appkey string,
        |  appregurl string,
        |  bdp_uuid string,
        |  createtime timestamp,
        |  isranreg string,
        |  regsource string,
        |  regsourcename string,
        |  websiteid int,
        |  dt string)
        |  using iceberg
        |  partitioned by(dt);
        |
        |""".stripMargin)

    sparkSession.sql(
      """
        |
        |create table hadoop_prod.db.dwd_base_ad(
        |adid int,
        |adname string,
        |dn string)
        |using iceberg
        |partitioned by (dn) ;
        |""".stripMargin)

    sparkSession.sql(
      """
        | create table hadoop_prod.db.dwd_base_website(
        |  siteid int,
        |  sitename string,
        |  siteurl string,
        | `delete` int,
        |  createtime timestamp,
        |  creator string,
        |  dn string)
        |using iceberg
        |partitioned by (dn) ;
        |""".stripMargin)

    sparkSession.sql(
      """
        |create table hadoop_prod.db.dwd_pcentermempaymoney(
        |  uid int,
        |  paymoney string,
        |  siteid int,
        |  vip_id int,
        |  dt string,
        |  dn string)
        |using iceberg
        | partitioned by(dt,dn);
        |""".stripMargin)

    sparkSession.sql(
      """
        | create table hadoop_prod.db.dwd_vip_level(
        |   vip_id int,
        |   vip_level string,
        |   start_time timestamp,
        |   end_time timestamp,
        |   last_modify_time timestamp,
        |   max_free string,
        |   min_free string,
        |   next_level string,
        |   operator string,
        |   dn string)
        | using iceberg
        | partitioned by(dn);
        |""".stripMargin)
    sparkSession.sql(
      """
        | create table hadoop_prod.db.dws_member(
        |  uid int,
        |  ad_id int,
        |  fullname string,
        |  iconurl string,
        |  lastlogin string,
        |  mailaddr string,
        |  memberlevel string,
        |  password string,
        |  paymoney string,
        |  phone string,
        |  qq string,
        |  register string,
        |  regupdatetime string,
        |  unitname string,
        |  userip string,
        |  zipcode string,
        |  appkey string,
        |  appregurl string,
        |  bdp_uuid string,
        |  reg_createtime timestamp,
        |  isranreg string,
        |  regsource string,
        |  regsourcename string,
        |  adname string,
        |  siteid int,
        |  sitename string,
        |  siteurl string,
        |  site_delete string,
        |  site_createtime string,
        |  site_creator string,
        |  vip_id int,
        |  vip_level string,
        |  vip_start_time timestamp,
        |  vip_end_time timestamp,
        |  vip_last_modify_time timestamp,
        |  vip_max_free string,
        |  vip_min_free string,
        |  vip_next_level string,
        |  vip_operator string,
        | dt string,
        |dn string)
        |using iceberg
        |partitioned by(dt,dn);
        |""".stripMargin)

    sparkSession.sql(
      """
        | create  table hadoop_prod.db.ads_register_appregurlnum(
        |  appregurl string,
        |  num int,
        |  dt string,
        |  dn string)
        |using iceberg
        |partitioned by(dt);
        |""".stripMargin)

    sparkSession.sql(
      """
        |create table hadoop_prod.db.ads_register_top3memberpay(
        |  uid int,
        |  memberlevel string,
        |  register string,
        |  appregurl string,
        |  regsourcename string,
        |  adname string,
        |  sitename string,
        |  vip_level string,
        |  paymoney decimal(10,4),
        |  rownum int,
        | dt string,
        |dn string)
        |using iceberg
        |partitioned by(dt);
        |""".stripMargin)
  }

}
