import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import scala.collection.JavaConverters._

object GlueApp {
  def main(args: Array[String]): Unit = {
    
    // Parse Glue Job arguments
    val glueArguments = GlueArgParser.getResolvedOptions(args, Seq("JOB_NAME").toArray)
    
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("ExperianRiskAnalysis")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    import spark.implicits._

    // Initialize Glue Context from SparkContext
    val glueContext: GlueContext = new GlueContext(spark.sparkContext)
    
    // Initialize and get Job reference
    val job = Job.init(glueArguments("JOB_NAME"), glueContext, glueArguments.asJava)

    try {
      // 1. Cargar datos crudos desde S3 (Bronze Layer)
      println("INFO: Leyendo datos desde S3 (Bronze Layer)...")
      val rawData = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "DROPMALFORMED") // Ignorar filas con formato incorrecto
        .csv("s3://experian-risk-data-geoffrey/raw/transactions.csv")

      println(s"INFO: Total de registros cargados: ${rawData.count()}")

      // 2. Validación y limpieza de datos (Data Quality)
      val cleanedData = rawData
        .filter($"client_id".isNotNull && $"amount".isNotNull && $"status".isNotNull)
        .filter($"amount" > 0) // Filtrar montos negativos o cero
        .withColumn("amount", round($"amount", 2)) // Redondear a 2 decimales

      println(s"INFO: Registros después de limpieza: ${cleanedData.count()}")

      // 3. Procesamiento y agregación (Silver Layer)
      val processedData = cleanedData
        .filter($"status" === "APPROVED")
        .groupBy("client_id")
        .agg(
          sum("amount").as("total_debt"),
          count("*").as("transaction_count"),
          avg("amount").as("avg_transaction_amount"),
          max("amount").as("max_transaction_amount"),
          min("amount").as("min_transaction_amount")
        )
        .withColumn("risk_level", 
          when($"total_debt" > 30000, "HIGH")
            .when($"total_debt" > 15000, "MEDIUM")
            .otherwise("LOW")
        )
        .withColumn("risk_score", 
          when($"risk_level" === "HIGH", 850)
            .when($"risk_level" === "MEDIUM", 650)
            .otherwise(450)
        )
        .withColumn("processing_date", current_date())
        .withColumn("processing_timestamp", current_timestamp())

      println(s"INFO: Clientes procesados: ${processedData.count()}")

      // 4. Guardar en formato Parquet optimizado (Silver Layer)
      println("INFO: Escribiendo datos procesados en S3 (Silver Layer)...")
      processedData
        .repartition(4) // Optimizar número de particiones
        .write
        .mode("overwrite")
        .option("compression", "snappy") // Compresión eficiente
        .partitionBy("risk_level") // Particionamiento por nivel de riesgo
        .parquet("s3://experian-risk-data-geoffrey/processed/risk_profiles/")

      println("INFO: Proceso completado exitosamente.")

      // Commit del job de Glue
      job.commit()

    } catch {
      case e: Exception =>
        println(s"ERROR: Falló el procesamiento - ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      spark.stop()
    }
  }
}