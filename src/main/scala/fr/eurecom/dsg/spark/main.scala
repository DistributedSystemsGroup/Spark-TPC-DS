import org.apache.spark.sql.parquet.Tables
import com.databricks.spark.sql.perf.tpcds.TPCDS

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object BenchSQLDS {
    def main(args: Array[String]) {
	val conf = new SparkConf().setAppName("Bench")
	val sparkContext = new SparkContext(conf)
	val sqlContext = new SQLContext(sparkContext)

	// Tables in TPC-DS benchmark used by experiments.
	val tables = Tables(sqlContext)
	// Setup TPC-DS experiment
	val tpcds =
	  new TPCDS (
	    sqlContext = sqlContext,
	    sparkVersion = "1.3.1",
	    dataLocation = "/user/ubuntu/tpcds-data",
	    dsdgenDir = "/usr/local/bin/",
	    tables = tables.tables,
	    scaleFactor = "10")

	tpcds.setup()

	tpcds.runExperiment(
	  queries = Seq(),
	  resultsLocation = "/user/ubuntu/tpcds-results",
	  includeBreakdown = true
//	  iterations = 1,
//	  variations = null,
//	  tags = <tags of this experiment>)
	)

    }
  }
