import com.amazonaws.services.services.glue.util.GlueArgParser
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkContext


object GlueApp {
    def main(sysArgs: Array[String]) {
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._

        val df = spark.read.option("header", "true").csv("s3://your-bucket/transactions.csv")

        val processedDf = df.filter($"status" === "APPROVED")
            .groupBy("client_id")
            .agg(sum("amount").as("total_amount"))
        
        processedDf.write.mode("overwrite").parquet("s3://your-bucket/processed_transactions/")

    }
}