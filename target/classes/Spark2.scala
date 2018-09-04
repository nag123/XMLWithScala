import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Spark2 extends App {
  
  val sparkConf = new SparkConf().setAppName("Spark over dataframe").setMaster("Local[*]")
  val spark = SparkSession.builder().appName("SQL over dataframe") .config("spark.master", "local").getOrCreate()
  
  val df = spark.read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true") 
    .option("nullValue","NA")
    .option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss")
    .option("mode","failfast")
    .load("F:\\my_learnings\\big data with mark\\survey123.csv")
  
 df.printSchema()
 println(df.schema)
 
 
import org.apache.spark.sql.types._

val columnSchema = StructType
(
 Array
	(
	StructField("Timestamp",TimestampType,true),
	StructField("age",LongType,true), 
	StructField("gender",StringType,true),
	StructField("country",StringType,true), 
	StructField("state",StringType,true),
	StructField("self_employed",StringType,true), 
	StructField("family_history",StringType,true),
	StructField("treatment",StringType,true), 
	StructField("work_interfere",StringType,true),
	StructField("no_employees",StringType,true), 
	StructField("remote_work",StringType,true),
	StructField("tech_company",StringType,true),
	StructField("benefits",StringType,true), 
	StructField("care_options",StringType,true),
	StructField("wellness_program",StringType,true), 
	StructField("seek_help",StringType,true), 
	StructField("anonymity",StringType,true), 
	StructField("leave",StringType,true), 
	StructField("mental_health_consequence",StringType,true),
	StructField("phys_health_consequence",StringType,true),
	StructField("coworkers",StringType,true),
	StructField("supervisor",StringType,true),
	StructField("mental_health_interview",StringType,true), 
	StructField("phys_health_interview",StringType,true),
	StructField("mental_vs_physical",StringType,true), 
	StructField("obs_consequence",StringType,true),
	StructField("comments",StringType,true)
	)
)



  val dfcol = spark.read
    .format("csv")
    .option("schema","columnSchema")
	  .option("header","true")
    .option("nullValue","NA")
    .option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss")
    .option("mode","failfast")
    .load("survey123.csv")
    
   val dfcolview = dfcol.createTempView("dfcolofsurvey")
   spark.catalog.listTables.show()
  
   
   val dfcolViewGlobal = dfcol.createGlobalTempView("dfcolsurveyglobal")
     spark.catalog.listTables.show()
   spark.catalog.listTables("global_temp").show()
  
   
   
   
    

}