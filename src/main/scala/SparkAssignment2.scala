import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object SparkAssignment2 extends App{

  //set the log level to print only errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  //create a sparkContext using every core of the local mechine
  val sc = new SparkContext("local[*]", "sparkAssinment2")

  //Load each line of source data into to RDD
  val logsData = sc.textFile("src/main/resources/ghtorrent-logs.txt")

  def parseLine(line: String): String = {
    val fields = line.split(",")
    fields(0)
  }

  def parseLine1(line: String): String = {
    val fields = line.split(",")
    fields(2)
  }

  def linesContainRDDFile(): Long ={
    val rddLines = logsData.map(parseLine)
    val lines = rddLines.collect()
    var count = 0
    for(i <- lines )
      count = count + 1
    //println(count)
    //another way to find no.of lines in rdd file
    rddLines.count()
  }

  def countWarningMessages(): Unit = {
    val rddLines = logsData.map(parseLine)
    val countWarnings = rddLines.filter(x => x == "WARN")
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y).foreach(println)
  }

  def countApiRepositories (): Unit ={
    val rdd = logsData.flatMap(x => x.split(" "))
      .filter(x => x == "api_client.rb:")
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y).foreach(println)
  }

  def countMaxHttp(): Unit ={
    val rdd1 = logsData.flatMap(x => x.split(","))
      .filter(x => x.contains("URL:") && x.contains("ghtorrent-"))
      .map(x => (x.substring(0, 13), 1))
      .reduceByKey((x, y) => x + y).sortBy(x => x._2, true).foreach(println)
  }

  def countFailedHtpp(): Unit = {
    val rdd1 = logsData.flatMap(x => x.split(","))
      .filter(x => x.contains("Failed") && x.contains("ghtorrent-"))
      .map(x => (x.substring(0, 13), 1))
      .reduceByKey((x, y) => x + y).foreach(println)
  }

  def countMostActiveHours() = {
    val rdd = logsData.flatMap(x => x.split(","))
      .filter(x => x.contains("+00"))
      .map(x => (x.substring(0, 11), ((x.substring(12,14)), 1)))
      .reduceByKey((x, y) => (x._1, x._2 + y._2))
      .foreach(println)
  }

  def countMostRepo() ={
    val reqfield = logsData.flatMap(x => x.split(","))
      .filter(x => x.contains("ghtorrent.rb: Repo") && x.contains("exists"))
      .map(x => (x.substring(x.indexOf("Repo")+5,x.indexOf("exists")-1),1))
      .reduceByKey((x,y) => x+y).sortBy(x => x._2,false).take(20)
      .foreach(println)
  }

  println("****** count the no.of lines contain rdd file *****")
  println(linesContainRDDFile())

  println("******* count the no.of Warning messages *******")
  countWarningMessages()

  println("******* count no.of Repositories **********")
  countApiRepositories()

  println("******** count no.of Max HTTP Requests ********")
  countMaxHttp()

  println("****** count the no.of failed Https ********")
  countFailedHtpp()

  println("***** most active time **** ")
  countMostActiveHours()

  println("******* count most repository **********")
  countMostRepo()
}
