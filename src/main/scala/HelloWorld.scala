// import required spark classes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
 
// define main method (Spark entry point)
object HelloWorld {
  def main(args: Array[String]) {
 
    // initialise spark context

    val conf = new SparkConf().setAppName(HelloWorld.getClass.getName)
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    //val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val dcDimSize = spark.sql("select count(*) from test_db.dc_dim_dev9")

    println("*******************************************************************************************")
    println("Length of table ")
    println(dcDimSize)

    println("*******************************************************************************************")
    val dc_dim = spark.sql("select * from test_db.dc_dim_dev9")
    println(dc_dim)
    println("*******************************************************************************************")
   // println("Length of table %d\n".format(dcDimSize))

    // do stuff
    println("************")
    println("************")
   // println("Hello, world!")
    //val rdd = sc.parallelize(Array(1 to 10))
    //rdd.count()
    //println("************")
    //println("************")
    
    // terminate spark context
    spark.stop()
    
  }
}

