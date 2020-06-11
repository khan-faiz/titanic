import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.classification.{SVMModel,SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils


object Titanic {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
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
    def nullPercent(title: String, x: Long) =
      s"$title: Found $x null entries, $totalrecords total entries, ${ 100*x.toFloat/totalrecords }% null"

    // Why is this var ಠ_ಠ
    var row = inputfile.take(5)(3)

    val nPID = nullChecks( inputfile, row.fieldIndex("PassengerId") )
    println(nullPercent("PID", nPID))

    val nPclass = nullChecks( inputfile, row.fieldIndex("Pclass") )
    println(nullPercent("PClass", nPclass))

    val nName = nullChecks( inputfile, row.fieldIndex("Name") )
    println(nullPercent("Name", nName))

    val nSex = nullChecks( inputfile, row.fieldIndex("Sex") ) //ಠ_ಠ
    println(nullPercent("Sex", nSex))

    val nAges = nullChecks( inputfile, row.fieldIndex("Age") )
    println(nullPercent("Age", nAges))

    val nSib = nullChecks( inputfile, row.fieldIndex("SibSp") )
    println(nullPercent("Siblings", nSib))

    val nParch = nullChecks( inputfile, row.fieldIndex("Parch") )
    println(nullPercent("Parchment", nParch))

    val nTick = nullChecks( inputfile, row.fieldIndex("Ticket") )
    println(nullPercent("Ticket", nTick))

    val nFare = nullChecks( inputfile, row.fieldIndex("Fare") )
    println(nullPercent("Fare", nFare))

    val nCabin = nullChecks( inputfile, row.fieldIndex("Cabin") )
    println(nullPercent("Cabin", nCabin))

    val nEmb = nullChecks( inputfile, row.fieldIndex("Embarked") )
    println(nullPercent("Embarked", nEmb))

  }

  def nullChecks(df: org.apache.spark.sql.DataFrame, index: Int) : Long = {
    df.filter( x => x( index ) == null ).count()
  }
}
