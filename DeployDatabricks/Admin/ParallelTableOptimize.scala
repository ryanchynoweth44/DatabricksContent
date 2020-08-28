// Databricks notebook source
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

spark.conf.set("spark.databricks.delta.checkpointV2.enabled", "true")
spark.conf.set("spark.databricks.delta.fastQueryPath.enabled", "true")
spark.conf.set("spark.databricks.delta.fastQueryPath.dataskipping.enabled", "true")

// COMMAND ----------

  def parOptimize(db: String, parallelism: Int,
                  zOrdersByTable: Map[String, Array[String]] = Map(),
                  vacuum: Boolean = true, retentionHrs: Int = 168): Unit = {
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 1024 * 1024 * 256)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    
    val tableList=spark.sessionState.catalog.listTables(db).toDF.map(x => x(0).toString).collect
    val tables = tableList
    val tablesPar = tables.par
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    tablesPar.tasksupport = taskSupport
    
    

    tablesPar.foreach(tbl => {
      try {
        val zorderColumns = if (zOrdersByTable.contains(tbl)) s"ZORDER BY (${zOrdersByTable(tbl).mkString(", ")})" else ""
        val sql = s"""optimize ${db}.${tbl} ${zorderColumns}"""
        println(s"optimizing: ${db}.${tbl} --> $sql")
        spark.sql(sql)
        if (vacuum) {
          println(s"vacuuming: ${db}.${tbl}")
          spark.sql(s"vacuum ${db}.${tbl} RETAIN ${retentionHrs} HOURS")
        }
        println(s"Complete: ${db}.${tbl}")
      } catch {
        case e: Throwable => println(e.printStackTrace())
      }
    })
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
  }


// COMMAND ----------

var databases = spark.sql("show databases").collect()

// COMMAND ----------

for (i < 0 to databases.length-1){
  println("Optimizing Database ---> " + databases(i)(0).toString)
  parOptimize(databases(i)(0).toString, 16, vacuum=false)
}
