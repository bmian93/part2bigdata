import org.apache.spark.{SparkConf, SparkContext}


object sort {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:/winutils")
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Word Count")
      .setSparkHome("src/main/resources")
    val sc = new SparkContext(conf)
    val arr = sc.parallelize(Array(2,3,47,3))
    val rdd=arr.map(x=>(x,1)).sortByKey()
    rdd.keys.collect.foreach(println)


  }

}
