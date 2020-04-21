import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object MainFile {
 
  def main(args: Array[String]) {

    // logs of the app is disabled
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val sparkConf = new SparkConf().setAppName("Titanic").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val textFile = sc.textFile("C:\\Users\\Akalanka\\Titanic_data.csv")
    averageAgeOfMalesAndFemales(textFile)
    numberofPeopleDiedClass(textFile)

  }

  
  def averageAgeOfMalesAndFemales(data:RDD[String]):Unit={
  
	//filtering the lines which has more than 7 columns
    val split = textFile.filter { x => {if(x.toString().split(",").length >= 6) true else false} }.map(line=>{line.toString().split(",")})
	
	//creating the key and values
    val key_value = split.filter{x=>if((x(1)=="1")&&(x(5).matches(("\\d+"))))true else false}.map(x => {(x(4),x(5).toInt)})
	
	//performing the average of the values for each unique key 
    key_value.mapValues((_, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues{ case (sum, count) => (1.0 * sum)/count}.collectAsMap()
	
    println("Average age of males and females who died in the Titanic tragedy")
	
    key_value.foreach(println)
  }

  
  def numberofPeopleDiedClass(data:RDD[String]):Unit= {
  
	//we are filtering the lines, which has more than 7 columns, so as to avoid ArrayIndexOutOfBound Exception after the filter.
    val split = textFile.filter { x => {if(x.toString().split(",").length >= 6) true else false} }.map(line=>{line.toString().split(",")})
	
	//clubbing the columns 2,5,6,3, which contains Survived, Gender, Age, Passenger class respectively
    val count=split.map(x=>(x(1)+" "+x(4)+" "+x(6)+" "+x(2),1)).reduceByKey(_+_).collect
	
    println("Number of people who died or survived in each class, along with their gender and age.")
	
    count.foreach(println)
  }

  

}