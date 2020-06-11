import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.classification.{SVMModel,SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils

object Titanic {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
   spark.sparkContext.setLogLevel("OFF")
    //val inputfile = sc.textFile("/home/faiz/Projects/Kaggle/Titanic/titanic/test.csv")
    val inputfile = spark.read.format("csv").option("sep",",").option("inferSchema","true").option("header","true").load("/home/faiz/titanic/train.csv")
    inputfile.printSchema()

    inputfile.filter( inputfile("Survived") === "1").show()

   // inputfile.filter( x => x( x.fieldIndex("Survived") ) == 1 ).show() // Alt way

    val totalrecords = inputfile.count()
    println(totalrecords)

    var row = inputfile.take(5)(3)
    println(row)
    println(row.fieldIndex("Age"))
    println(row( row.fieldIndex("Age") ) )
    println(row( row.fieldIndex("Age") )  == null )
    println( inputfile.filter( x => x( x.fieldIndex("Age") ) == null ).getClass)

  
    val noage = inputfile.filter( x => x( x.fieldIndex("Age") ) == null )

    println(noage)
    println("test")
    println(noage.take(5))
    noage.show() 
    //println(noage)
    val nocabin = inputfile.filter( x=> x( x.fieldIndex("Cabin") ) == null )
    nocabin.show()
    println(nocabin)

    println(totalrecords)
/*    println(noage/totalrecords)
    println(nocabin/totalrecords)

    inputfile.filter( x=> x( x.fieldIndex("Cabin") ) == null ).count()*/

  }
}
