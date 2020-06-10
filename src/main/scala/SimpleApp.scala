/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession


/*
inputfile.first()
inputfile.take(5)
inputfile.take(5)[0]
inputfile.take(5)(0)
inputfile.take(5)(1)
inputfile.take(5)(2)
val inputfile = sc.textFile("train.csv")
inputfile.take(5)(0)
inputfile.take(5)(1)
inputfile.take(5)(2)
val inputfile = spark.read.format("csv").option("sep","\n").option("inferSchema","true").option("header","true").load("train.csv")
inputfile.take(5)(1)
inputfile.take(5)(1)(2)
inputfile.take(5)(1)
inputfile.take(5)(0)
inputfile.printSchema()
inputfile.show()
inputfile.select()
inputfile.select("*")
inputfile.filter

inputfile.select("Survived")
inputfile.select(Survived)
inputfile.select("Name")
inputfile.printSchema()

inputfile.printSchema()
inputfile.select("Name")
inputfile.filter( inputfile("Survived") == "1").show()
inputfile.filter( inputfile("Survived") == 1).show()
inputfile("Survived")
inputfile.filter( inputfile("Survived") = 1).show()
inputfile.filter( inputfile("Survived") > 0).show()
inputfile.filter( inputfile("Survived").equals(1)).show()
inputfile.filter( inputfile("Survived") = 1).show()
inputfile.filter( inputfile("Survived") == 1).show()
inputfile.filter( inputfile("Survived") > 0).show()
inputfile("Survived")
inputfile.printSchema()
inputfile.filter( inputfile("Survived") >= 0).show()
inputfile.filter( inputfile("Survived") == 0).show()
inputfile.filter( _("Survived") == 0).show()
inputfile.take(5)(0)
inputfile.take(5)(0)(1)
inputfile.take(5)(0)(0)
inputfile.filter( _(1) == 0).show()
inputfile.filter( _(1) == 1).show()
inputfile.filter( aaa(1) == 1).show()
inputfile.filter( _(1) == 1).show()
inputfile.filter( inputfile("Survived") === 0).show()
inputfile.filter( inputfile("Survived") === 1).show()
inputfile.filter( "Survived" == 1).show()
inputfile.filter( "Survived" == '1').show()
inputfile.filter( "Survived == '1'").show()
inputfile.filter( (x: Boolean) => x(1) == 1 ).show()
inputfile.filter( x => x(1) == 1 ).show()
inputfile.filter( x => x("Survived") == 1 ).show()
inputfile.take(5)(0)(1)
inputfile.take(5)
inputfile.take(5)(2)
inputfile.take(5)(0).getAs("Survived")
inputfile.take(5)(0)
inputfile.take(5)(0).getAs("Name")
inputfile.take(5)(0).getAs[Int]("Survived")
inputfile.take(5)(0).getAs[String]("Name")
inputfile.take(5)(0).getAs("Name")
inputfile.filter( x => x.getAs[Int]("Survived") == 1 ).show()
inputfile.filter( x => x( x.fieldIndex("Survived") ) == 1 ).show()*/

object SimpleApp {
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
