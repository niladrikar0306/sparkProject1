import org.apache.spark.sql._

object IndexedRows extends App {

  case class Index(row: Int, col: Int)

  private val APP_NAME = "Spark Project 1"
  private val PATH = "data/data1.csv"
  private val MASTER = "local[*]"

  println("hello")

  val spark = SparkSession.builder.appName(APP_NAME).master(MASTER).getOrCreate

  // Read file
  //  val data = spark.read.format("csv").load(PATH)

  val data = spark.read.option("inferSchema", true).csv(PATH).collect

  val dataSeq = data.map(row => Array(row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getInt(4)))

  val a = for {
    i <- 0 until dataSeq.length
    j <- 0 until dataSeq(i).length
    if (dataSeq(i)(j) <= 40)
  } yield Index(i, j) -> dataSeq(i)(j)

  println(a.length)
  println(a.toMap)


}
