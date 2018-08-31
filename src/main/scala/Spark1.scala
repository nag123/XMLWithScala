
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.when
import org.apache.spark.sql._
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.UDFRegistration
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.DateType
import java.util.Date
import java.util.Locale


object Spark1 extends App
{
val conf = new SparkConf().setAppName("Wordcount")
                          .setMaster("local[*]")
                          
val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.master", "local")
  .getOrCreate()
  
  
  import spark.implicits._ 
  
  val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
  .option("mode", "failfast")
  .option("path", "survey.csv")
  .load()
    spark.conf.set("spark.shuffle.partitions", 2)
  val df0 = df.select($"Gender", $"treatment")
  df0.printSchema()
  println("************")
  df0.show()
  
 /* val df1 = df0.select($"Gender"
      (when($"treatment" === "Yes" ,1).otherwise(0)).alias("All-Yes"),
      (when($"treatment" === "No" ,1).otherwise(0)).alias("All-No"))
   println("*************df1 value *******************")
  df1.printSchema()
  df1.show()   
 
  */
  val df2 = df.groupBy($"Gender").agg(sum(when($"treatment" === "Yes",1).otherwise(0)), sum(when($"treatment" === "No",1).otherwise(0)))
  println("***********df2 values ****************")    
  df2.show()
  
  def parseGender(g:String) =
  {
  g.toLowerCase() match
  {
    case "male" | "m" | "male-ish" | "cis man" | "man" | "male (cis)" | "make" | "malr" | "mail" => "Men"
    case "female" | "f" | "cis female" | "femake" | "woman" | "female (cis)" | "femail" => "Women"
    case _ => "TransGender"
  }
  }

val parseGenderUDF = udf(parseGender _)

val df1 = df0.select(parseGenderUDF($"Gender").alias("Gendu"),
      (when($"treatment" === "Yes" ,1).otherwise(0)).alias("All-Yes"),
      (when($"treatment" === "No" ,1).otherwise(0)).alias("All-No"))
   println("*************df1 value *******************")
  df1.printSchema()
  df1.show()   

  
  val df4 = df1.groupBy($"Gendu").agg(sum($"All-Yes"),sum($"All-No"))
  println("df4 values **********************")
  df4.printSchema()
  df4.show()
  
  val df5 = df.withColumn("Timestamp",to_date(unix_timestamp(df.col("Timestamp"), "MM-dd-yyyy'T'HH:mm").cast("timestamp")))
  df5.printSchema()
  df5.show()

  
  println("*******************************************dframeee****************************************************")
 val dframeee = df.withColumn("Timestamp",to_date(unix_timestamp(df.col("Timestamp")).cast("timestamp")))
  dframeee.show()

  val ts1234 = unix_timestamp($"Timestamp", "dd-MMM-yy").cast(TimestampType)
  df.withColumn("test", ts1234).show()

  
  
  import java.text.SimpleDateFormat
def parseDateinDiff(data : String) : String = 
{
  val d123 = List("dd-MMM","yyyy-MM-dd","MM/dd/yyyy T HH:mm:ss a","MM/dd/yyyy","MM/dd/yyyy T HH:mm:ss","mm/dd/yyyy T HH:mm",
    "M/dd/yyyy hh:mm", "mm/dd/yyyy hh:mm" , "M/dd/yyyy" )
val v : String = date_format(col("Timestamp"),"yyyy-MM-dd").toString()
 
val inputFormat = 
 v match
  {
    case "dd-MMM" => new SimpleDateFormat("dd-MMM")
    case "yyyy-MM-dd" => new SimpleDateFormat("yyyy-MM-dd")
    case "MM/dd/yyyy T HH:mm:ss a" => new SimpleDateFormat("MM/dd/yyyy T HH:mm:ss a")
   case "MM/dd/yyyy" => new SimpleDateFormat("MM/dd/yyyy,Locale.ENGLISH")
   case "MM/dd/yyyy T HH:mm:ss" => new SimpleDateFormat("MM/dd/yyyy T HH:mm:ss")
   case "mm/dd/yyyy T HH:mm" => new SimpleDateFormat("mm/dd/yyyy T HH:mm")
  case "M/dd/yyyy hh:mm" => new SimpleDateFormat("M/dd/yyyy hh:mm")
  case "mm/dd/yyyy hh:mm" => new SimpleDateFormat("mm/dd/yyyy hh:mm")
  case "M/dd/yyyy" => new SimpleDateFormat("M/dd/yyyy",Locale.ENGLISH)
   
  }
  
val d : Date  = inputFormat.parse(data)

val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm")
val formattedDate = outputFormat.format(d)

println(formattedDate) 
formattedDate
}

  val parseDateUDF = udf(parseDateinDiff _)

val df12 =  df.select(parseDateUDF($"Timestamp"))
   println("*************df12 value *******************")
   df12.show()
  df12.printSchema()

  
  
  
  
  


import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.date_format
val ts = unix_timestamp($"Timestamp", "dd-MMM-yy").cast(TimestampType)
println("***************************************the trouble some ******************** ;;;;;;;;;;;;;;;")
def parseDateUDF( formats : String)  =
{ val ts = formats match
  {
    case "dd-MMM" => unix_timestamp($"Timestamp", "dd-MMM").cast(TimestampType)
    case "yyyy-MM-dd" => unix_timestamp($"Timestamp", "yyyy-MM-dd").cast(TimestampType)
    case "MM/dd/yyyy T HH:mm:ss a" => unix_timestamp($"Timestamp", "MM/dd/yyyy T HH:mm:ss a").cast(TimestampType)
   case "MM/dd/yyyy" => unix_timestamp($"Timestamp", "MM/dd/yyyy").cast(TimestampType)
   case "MM/dd/yyyy T HH:mm:ss" => unix_timestamp($"Timestamp", "MM/dd/yyyy T HH:mm:ss").cast(TimestampType)
   case "mm/dd/yyyy T HH:mm" => unix_timestamp($"Timestamp", "MM/dd/yyyy T HH:mm").cast(TimestampType)
  case "M/dd/yyyy hh:mm" => unix_timestamp($"Timestamp", "M/dd/yyyy hh:mm").cast(TimestampType)
  case "mm/dd/yyyy hh:mm" => unix_timestamp($"Timestamp", "mm/dd/yyyy hh:mm").cast(TimestampType)
   
  }
ts.toString()
 }

//
//val parseDateUDF12 = udf(parseDateUDF _)
//
//val df12 = df.select(parseDateUDF12(col("Timestamp")).cast("String")).toDF("Timestamp")
//   println("*************df12 value *******************")
//   df12.show()
//  df12.printSchema()

val changeDtFmt = udf{(cFormat: String,
                         rFormat: String,
                         date: String) => {
  val formatterOld = new SimpleDateFormat(cFormat)
  val formatterNew = new SimpleDateFormat(rFormat)
  formatterNew.format(formatterOld.parse(date))
}}
println("***************************************")
df.
  withColumn("Timestamp", 
    changeDtFmt(lit("dd/mm/yyyy"), lit("yyyy-MM-dd HH:mm:ss"), $"Timestamp"))
  

}