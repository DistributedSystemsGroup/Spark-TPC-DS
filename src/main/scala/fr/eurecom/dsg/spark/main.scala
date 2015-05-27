import scala.concurrent.Await
import scala.concurrent.duration._

import org.apache.spark.sql.parquet.Tables
import com.databricks.spark.sql.perf.tpcds.TPCDS
import com.databricks.spark.sql.perf.tpcds.queries.{SimpleQueries, ImpalaKitQueries}

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
	    dataLocation = args(0),
	    dsdgenDir = "/usr/local/bin/",
	    tables = tables.tables,
	    scaleFactor = args(1))

	tpcds.setup()

	var queries = SimpleQueries.q7Derived
	if (args(4) == "impalakit") {
		queries = ImpalaKitQueries.impalaKitQueries
	} else if (args(4) == "interactive") {
		queries = ImpalaKitQueries.interactiveQueries
	} else if (args(4) == "reporting") {
		queries = ImpalaKitQueries.interactiveQueries
	} else if (args(4) == "deepAnalytics") {
		queries = ImpalaKitQueries.interactiveQueries
	} else if (args(4) == "simple") {
		queries = SimpleQueries.q7Derived
	}

	// ImpalaKitQueries contains:
	// - interactiveQueries
	// - reportingQueries
	// - deepAnalyticQueries
	// - impalaKitQueries (all of the above)
	
	// val queries = SimpleQueries
	// SimpleQueries contains:
	// - q7Derived (five variations of the q7 query)
	val exp = tpcds.runExperiment(
	  queries = queries,
	  resultsLocation = args(2),
	  includeBreakdown = true,
	  iterations = args(3).toInt
//	  variations = null,
//	  tags = <tags of this experiment>
        )
	do {
		println(exp)
		Thread.sleep(2000)
	} while(exp.status == "Running")
	println(exp)

	sparkContext.stop()
    }
  }
