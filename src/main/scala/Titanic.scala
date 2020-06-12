import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.classification.{SVMModel,SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector,Vectors}
import org.apache.log4j.Logger
import org.apache.log4j.Level


case class VectorRecord( label: Int, features: org.apache.spark.ml.linalg.Vector)

object Titanic {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("OFF")
    val input = spark
      .read
      .format("csv")
      .option("sep",",")
      .option("inferSchema","true")
      .option("header","true")
      .load("src/main/resources/train.csv")
    input.printSchema()

    println(input.getClass)
    //basicAnalysis(input)
    val transformed = inputMassage(input)
    //val transformed = input.map( row => ( row( row.fieldIndex("Survived") ), 1 ) )
    //val transformed = input.map( row => ( 1 , 1 ) )
    println(transformed)


  }
  def inputMassage(input: org.apache.spark.sql.DataFrame) = {
     //println(input.take(4)(2)( input.take(4)(2).fieldIndex("Survived")))
     //println(input.take(4)(2)( input.take(4)(2).fieldIndex("Survived")).getClass)
     //input.map( row => ( row( row.fieldIndex("Survived") ), row( row.fieldIndex("PassengerID") ) ) )
     //input.map( row => ( row( row.fieldIndex("Survived") ), Vectors.dense(1.0) ):(Int, Vector) )
     input.map( row => ( 4, Vectors.dense(1.0) ):(Int, Vector) )

  }

  def basicAnalysis(input: org.apache.spark.sql.DataFrame) {
    input.filter( input("Survived") === "1").show()

    val totalrecords = input.count()

    def nullPercent(title: String, x: Long) =
      s"$title: Found $x null entries, $totalrecords total entries, ${ 100*x.toFloat/totalrecords }% null"

    var columns = input.columns

    val nulls = input.columns.map( column => ( column, input.filter( row => row( columns.indexOf(column) ) == null ).count() ) ) 

    val nullstrings = nulls.map( x  => nullPercent(x._1, x._2) )
    nullstrings.foreach( println )
  }

}
