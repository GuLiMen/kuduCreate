package util.kudu_export

import org.apache.kudu.spark.kudu.{KuduContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
object oracle {




  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("OracleTest")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
      .getOrCreate()


    val kuduMaster = "bigdata01,bigdata02,bigdata03"
    val table = "ERP_PSAAUC_T"
    val sc = spark.sparkContext
    sc.setLogLevel("warn")
    val kuduContext = new KuduContext(kuduMaster, sc)

    val reader: DataFrame = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@//10.170.1.196:1521/topprd")
      .option("user", "dsdata")
      .option("password", "dsdata")
      .option("dbtable", "dsdata.psaauc_t")
      .load()


//    reader.write.format("org.apache.kudu.spark.kudu")
//      .option("kudu.master", "bigdata01,bigdata02,bigdata03")
//      .option("kudu.table", "ERP_PSAAUC_T")
//      .mode("append").save()


    val OptionAds_qtlsch_t = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> table
    )

    spark.read.options(OptionAds_qtlsch_t)
      .format("kudu")
      .load.createOrReplaceTempView("ERP_PSAAUC_T")

    reader.createOrReplaceTempView("tmp1")

    saveData2Kudu(table,spark,reader)

     // spark.sql("select * from tmp2")
//    spark.sql("select * from tmp1").createOrReplaceTempView("tmp2")
//
//    spark.sql(s"insert into ERP_PSAAUC_T select * from tmp2")
//
//      spark.close()
    
  }



  def saveData2Kudu(tableName : String, sparkSession: SparkSession, dataset: DataFrame) = {
    val kuduContext = new KuduContext("bigdata01,bigdata02,bigdata03",sparkSession.sparkContext)
    kuduContext.upsertRows(dataset,tableName)
    true
  }


}
