
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Spark3 extends App {
  val sparkConf = new SparkConf().setAppName("Spark over dataframe").setMaster("Local[*]")
  val spark = SparkSession.builder().appName("Read and write data in dataframe") .config("spark.master", "local").getOrCreate()
  
  val df1 = spark.read
                 .format("csv")
                .option("header","true")
                .option("inferSchema","true") 
                .option("nullValue","NA")
                .option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss")
                .option("mode","failfast")
                .load("survey123.csv")
                
   df1.printSchema()
   
   //write format
   val df2 = df1.write
                 .format("parquet")
                 .mode("overwrite")
                 .save("x1")
                 
      
              
}