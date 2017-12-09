import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Location_Vertify {

  case class result(catogries: String, value: Double)

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val sparkConf = new SparkConf().setAppName("LocationVerify_126").setMaster("yarn-client")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val otherArgs: Array[String] = new GenericOptionsParser(conf, args).getRemainingArgs()
    var input1 = otherArgs(0)
    var input2 = otherArgs(1)
    var output1 = otherArgs(2)
    var output2 = otherArgs(3)
    var output3 =otherArgs(4)

//        val input1 = "E:\\workplace\\fanbo_profile\\00-mr_orig_omc100_1511321518121_mr_original_l_381.dat"
//        val input2 = "E:\\workplace\\fanbo_profile\\basestation\\cellHA.txt"
    //    val output = "E:\\workplace\\fanbo_profile\\test1"
    var fs: FileSystem = FileSystem.get(conf)
    if (fs.exists(new Path(output1))) {
      fs.delete(new Path(output1))
    }
    if (fs.exists(new Path(output2))) {
      fs.delete(new Path(output2))
    }
    if (fs.exists(new Path(output3))) {
      fs.delete(new Path(output3))
    }
    //    var base = sc.textFile(input2)//.map(_.split(","))
    //    var baseRDD = base.mapPartitionsWithIndex((idx, iter) =>
    //      if (idx == 0) iter.drop(1)
    //      else iter
    //    )
    val getdistance = udf { (glon1: Double, glat1: Double, lon2: Double, lat2: Double) =>
//      val R = 6371.137
  //      val PI = 3.1415926
  //      var a1 = glat1 * PI / 180.0
  //      var a2 = glon1 * PI / 180.0
  //      var b1 = lat2 * PI / 180.0
  //      var b2 = lon2 * PI / 180.0
  //      var dlon: Double = b2 - a2
  //      var dlat: Double = b1 - a1
  //      var t1: Double = Math.pow(Math.sin(dlat / 2.0), 2) + Math.cos(a1) * Math.cos(b1) * Math.pow(Math.sin(dlon / 2), 2)
  //      var t2: Double = 2 * Math.asin(Math.sqrt(t1))
  //      var t3: Double = R * t2
  //      var distance = t3 * 1000
  //      distance.formatted("%.4f").toDouble
        val R = 6378.137
        val PI = 3.1415926
        var a1 = glat1 * PI / 180.0
        var a2 = glon1 * PI / 180.0
        var b1 = lat2 * PI / 180.0
        var b2 = lon2 * PI / 180.0
        var t1: Double = Math.cos(a1) * Math.cos(a2) * Math.cos(b1) * Math.cos(b2)
        var t2: Double = Math.cos(a1) * Math.sin(a2) * Math.cos(b1) * Math.sin(b2)
        var t3: Double = Math.sin(a1) * Math.sin(b1)
        var distance = Math.acos(t1 + t2 + t3) * R
        distance.formatted("%.4f").toDouble * 1000
    }

    val base = sc.textFile(input2).map(_.split("\t")).filter(line => !(line(2).equals("")) && !(line(3).equals("")) && (line(8).length > 3) && (line(9).length > 3))
      .map(x => (x(2).trim.toDouble, x(3).trim.toDouble, x(9).trim.toDouble, x(8).trim.toDouble))
    val df2 = base.toDF("enbid", "pci", "base_lon", "base_lat")

    val mrRDD = sc.textFile(input1).map(_.split("~", -1))
      .filter(line => (line.length >= 100) && ((!line(5).equals("")) && (line(5).length > 3)) && ((!line(6).equals("")) && (line(6).length > 3)) && ((!line(115).equals("")) && (line(115).length > 3)) && ((!line(116).equals("")) && (line(116).length > 3)) && (!line(17).equals("")) && (!line(57).equals("")))
      .map { line =>
        val agps_lon = line(5).trim.toDouble
        val agps_lan = line(6).trim.toDouble
        val enbid = line(17).trim.replaceAll("LTE.", "").toDouble
        val pci = line(57).toDouble
        val triangle_lon = line(115).trim.toDouble
        val triangle_lan = line(116).trim.toDouble
        //        var distance1 = getDistance(agps_lon,agps_lan,triangle_lon,triangle_lan)
        val nearList = new scala.collection.mutable.ListBuffer[String]
        var i = 59
        while (i < 91) {
          nearList.append(line(i))
          i = i + 1
        }
        //        distance1+"\t"+agps_lon+"\t"+agps_lan+"\t"+triangle_lon+"\t"+triangle_lan+"\t"+ enbid + "\t" + pci +"\t" + nearList.mkString("\t")
        (enbid, pci, agps_lon, agps_lan, triangle_lon, triangle_lan, nearList.mkString("\t"))
      }.repartition(200)
    val df1 = mrRDD.toDF("enbid", "pci", "agps_lon", "agps_lat", "triangle_lon", "triangle_lat", "nearList")

    var df3 = df1.join(df2, Seq("enbid", "pci"), "inner")
    df3 = df3.withColumn("dis_base", getdistance($"agps_lon", $"agps_lat", $"base_lon", $"base_lat"))
    val total = df3.count().toDouble
    var df4 = df3.filter("dis_base < 1000")
    val fil = df4.count().toDouble
    df4 = df4.withColumn("dis_fact", getdistance($"agps_lon", $"agps_lat", $"triangle_lon", $"triangle_lat"))
    //      val schema = org.apache.spark.sql.types.StructType(Array(org.apache.spark.sql.types.StructField("B200", org.apache.spark.sql.types.DoubleType, false), org.apache.spark.sql.types.StructField("B500", org.apache.spark.sql.types.DoubleType, false), org.apache.spark.sql.types.StructField("B1000", org.apache.spark.sql.types.DoubleType, false),org.apache.spark.sql.types.StructField("A1000", org.apache.spark.sql.types.DoubleType, false), org.apache.spark.sql.types.StructField("filter", org.apache.spark.sql.types.DoubleType, false),org.apache.spark.sql.types.StructField("total", org.apache.spark.sql.types.DoubleType, false)))
//    val Row = List(result("B200", df4.filter("dis_base < 1000 and dis_fact < 200").count().toDouble / df4.filter("dis_base < 1000").count()),
//      result("B500", df4.filter("dis_base < 1000 and dis_fact < 500").count().toDouble / df4.filter("dis_base < 1000").count()),
//      result("B1000", df4.filter("dis_base < 1000 and dis_fact < 1000").count().toDouble / df4.filter("dis_base < 1000").count()),
//      result("B2000", df4.filter("dis_base < 1000 and dis_fact < 2000").count().toDouble / df4.filter("dis_base < 1000").count()),
//      result("A1000", df4.filter("dis_base < 1000 and dis_fact > 1000").count().toDouble / df4.filter("dis_base < 1000").count()),
//      result("base_clean", df4.filter("dis_base < 1000").count().toDouble), result("total", df4.count().toDouble))
    val df_problem = df4.filter("dis_fact > 1000")
    val Row = List(result("B200", df4.filter("dis_fact < 200").count().toDouble / fil),
  result("B500", df4.filter("dis_fact < 500").count().toDouble / fil),
  result("B1000", df4.filter("dis_fact < 1000").count().toDouble / fil),
  result("B2000", df4.filter("dis_fact < 2000").count().toDouble / fil),
  result("A1000", df4.filter("dis_fact > 1000").count().toDouble / fil),
  result("base_clean", fil), result("total", total))

    val df_result = sqlContext.createDataFrame(Row)
//    df_result.show()
//    df4.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save(output1)
    df4.rdd.saveAsTextFile(output1)
//    df_result.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save(output2)
    df_result.rdd.repartition(1).saveAsTextFile(output2)
//    df_problem.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save(output3)
    df_problem.rdd.saveAsTextFile(output3)
//    df_result.foreach(println)
    sc.stop()
  }
}
