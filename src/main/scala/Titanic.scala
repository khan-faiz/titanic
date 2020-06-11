import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.classification.{SVMModel,SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils

object Titanic {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
   spark.sparkContext.setLogLevel("OFF")
    //val inputfile = sc.textFile("/home/faiz/Projects/Kaggle/Titanic/titanic/test.csv")
    val inputfile = spark
      .read
      .format("csv")
      .option("sep",",")
      .option("inferSchema","true")
      .option("header","true")
      .load("src/main/resources/train.csv")
    inputfile.printSchema()

    println(inputfile.getClass)
    basicAnalysis(inputfile)

  }
  def basicAnalysis(inputfile: org.apache.spark.sql.DataFrame) {
    inputfile.filter( inputfile("Survived") === "1").show()

    val totalrecords = inputfile.count()

    var row = inputfile.take(5)(3)

    val nPID = nullChecks( inputfile, row.fieldIndex("PassengerId") )
    println(s"PID: Found $nPID null entries, $totalrecords total entries, ${ 100*nPID.toFloat/totalrecords }% null")

    val nPclass = nullChecks( inputfile, row.fieldIndex("Pclass") )
    println(s"Pclass: Found $nPclass null entries, $totalrecords total entries, ${ 100*nPclass.toFloat/totalrecords }% null")
  
    val nName = nullChecks( inputfile, row.fieldIndex("Name") )
    println(s"Name: Found $nName null entries, $totalrecords total entries, ${ 100*nName.toFloat/totalrecords }% null")

    val nSex = nullChecks( inputfile, row.fieldIndex("Sex") ) //ಠ_ಠ
    println(s"Sex: Found $nSex null entries, $totalrecords total entries, ${ 100*nSex.toFloat/totalrecords }% null")

    val nAges = nullChecks( inputfile, row.fieldIndex("Age") )
    println(s"Ages: Found $nAges null entries, $totalrecords total entries, ${ 100*nAges.toFloat/totalrecords }% null")

    val nSib = nullChecks( inputfile, row.fieldIndex("SibSp") )
    println(s"Siblings: Found $nSib null entries, $totalrecords total entries, ${ 100*nSib.toFloat/totalrecords }% null")

    val nParch = nullChecks( inputfile, row.fieldIndex("Parch") )
    println(s"Parchment: Found $nParch null entries, $totalrecords total entries, ${ 100*nParch.toFloat/totalrecords }% null")

    val nTick = nullChecks( inputfile, row.fieldIndex("Ticket") )
    println(s"Ticket: Found $nTick null entries, $totalrecords total entries, ${ 100*nTick.toFloat/totalrecords }% null")

    val nFare = nullChecks( inputfile, row.fieldIndex("Fare") )
    println(s"Fare: Found $nFare null entries, $totalrecords total entries, ${ 100*nFare.toFloat/totalrecords }% null")

    val nCabin = nullChecks( inputfile, row.fieldIndex("Cabin") )
    println(s"Cabin: Found $nCabin null entries, $totalrecords total entries, ${ 100*nCabin.toFloat/totalrecords }% null")

    val nEmb = nullChecks( inputfile, row.fieldIndex("Embarked") )
    println(s"Embarked: Found $nEmb null entries, $totalrecords total entries, ${ 100*nEmb.toFloat/totalrecords }% null")

  }
  def nullChecks(df: org.apache.spark.sql.DataFrame, index: Int) : Long = {
    df.filter( x => x( index ) == null ).count()
  }
}
