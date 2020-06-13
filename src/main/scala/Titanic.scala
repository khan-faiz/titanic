import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.classification.{SVMModel,SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector,Vectors}
import org.apache.spark.sql.{Row, Encoders, Encoder}
import org.apache.log4j.Logger
import org.apache.log4j.Level


case class VectorRecord( label: Int, features: org.apache.spark.ml.linalg.Vector)

object Titanic {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    implicit val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
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
    basicAnalysis(input)

    val inputlist = input.takeAsList( input.count().toInt ).asScala.toList
    
    //println(massaged.mkString(" "))
    //massaged.toDS().show()
    
    val transformed = inputMassage(inputlist)
    val inputrdd = spark.sparkContext.parallelize(transformed,1)
    inputrdd.take(4).foreach(println)
    //val transformed2 = transformed.map( x=> (x._1, Vectors.dense(1) ) )

    //t2.foreach(println)
  }
  def inputMassage(input: List[org.apache.spark.sql.Row] ) = {
    
    input.map { row =>
      val label = row.getInt(row.fieldIndex("Survived")) 
      val featureCols = row.schema.fieldNames.filter( x => ( (x != "Survived") && (x != "Name") && (x != "Age") && (x != "Cabin") && (x != "Ticket") && (x != "Embarked") ) ) 
      // need to properly handle embarked, age, and ticket at some point instead of throwing it away, TODO
      (label, row.getValuesMap(featureCols) )
    }.map{ row => 
        val sex = if (row._2("Sex") == "male") 1 else 2
        (row._1, Vectors.dense( row._2("PassengerId"):Int, row._2("Pclass"):Int, row._2("SibSp"):Int, row._2("Parch"):Int,row._2("Fare") , sex ) ) 
    }

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
