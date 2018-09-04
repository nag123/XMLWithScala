
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import com.databricks.spark._

object Spark3 extends App {
  val sparkConf = new SparkConf().setAppName("Spark over dataframe").setMaster("Local[*]")
  val spark = SparkSession.builder().appName("Read and write data in dataframe") .config("spark.master", "local").getOrCreate()

  
  val df1 = spark.read
                 .format("csv")
                .option("header","true")
                .option("nullValue","NA")
                 .option("inferSchema", "true")
                .option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss")
                .option("mode","failfast")
                .load("survey123.csv")
                
   df1.printSchema()
   
   //write format
 df1.write
                 .format("parquet")
                 .mode("overwrite")
                 .save("parquetdata")
                 
          
                 
     val df3 = spark.read
                 .format("parquet")
                .option("header","true")
               .option("nullValue","NA")
                .option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss")
                .option("mode","failfast")
                .load("parquetdata")
                
   df3.printSchema()
   println("printed the parquet data")
   //write format
                  df3.write
                 .format("com.databricks.spark.xml")
                 .option("rowTag","Transaction")                
                 .mode("overwrite")
                 .save("xmldata")
                 
                 
                 
 //read from xml and write to a csv
                 
              val sc =  spark.sparkContext // An existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._

val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "Transaction").load("xmldata")
                
                 df.write.
    format("csv").
    option("header", "true").
    save("out.csv")
    
              
}