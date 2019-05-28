package org.niladri.sparkproject1

import org.apache.spark.sql.SparkSession

trait SparkSessionAware {
  private val APP_NAME = "Spark Project 1"
  private val MASTER = "local[*]"

  def sparkSession = SparkSession.builder.appName(APP_NAME).master(MASTER).getOrCreate
}
