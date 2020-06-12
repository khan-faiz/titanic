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

    /* Scala has no problem handling this, its only when trying to map complex types back to DataFrame/Datasets aka spark
       Spark's map method is experimental 
    val listtest = List(1,2,3,4).map( x=> (x, (x+1,x+2)))
    println(listtest)
    */


    val inputlist = input.takeAsList( input.count().toInt ).asScala.toList
    val itransform = inputlist.map( row => (1, Vectors.dense(1)) )
    itransform.foreach(x => println(x))
/*
    import spark.implicits._
    val a = Seq( (1,Vectors.dense(1)), (0, Vectors.dense(2) ) ).toDS()
    a.show()
    var massaged = Seq(1,Vectors.dense(9))
    massaged :+ (2,Vectors.dense(10))
    input.foreach( row => massaged :+ (1,Vectors.dense(1)) )


    def createList( input: org.apache.spark.sql.DataFrame, sequence ) {
       return sequence :+ createList( input
       
    }
*/
    //println(massaged.mkString(" "))
    //massaged.toDS().show()
    
    val transformed = inputMassage(inputlist)
    transformed.foreach(println)
    //val transformed = input.map( row => ( row( row.fieldIndex("Survived") ), 1 ) )
    //val transformed = input.map( row => ( 1 , 1 ) )
    //transformed.show()
    //transformed.take(5).foreach(println)

    /*val vectors = transformed.foreach { row => 
      val features = row(1)
      print(features)
    }
    println(vectors)  */

  }
  def inputMassage(input: List[org.apache.spark.sql.Row] ) = {
     //println(input.take(4)(2)( input.take(4)(2).fieldIndex("Survived")))
     //println(input.take(4)(2)( input.take(4)(2).fieldIndex("Survived")).getClass)
     //input.map( row => ( row( row.fieldIndex("Survived") ), row( row.fieldIndex("PassengerID") ) ) )
     //input.map( row => ( row( row.fieldIndex("Survived") ), Vectors.dense(1.0) ):(Int, Vector) )
    
    input.map { row =>
      val label = row.getInt(row.fieldIndex("Survived")) 
      //val features = row.getValuesMap( featureCols ).mkString(" ")
      //val features = row.getValuesMap( featureCols )
      val featureCols = row.schema.fieldNames.filter( x => ( (x != "Survived") && (x != "Name") && (x != "Age") && (x != "Cabin") ) ) 

      (label, row.getValuesMap(featureCols) )
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
