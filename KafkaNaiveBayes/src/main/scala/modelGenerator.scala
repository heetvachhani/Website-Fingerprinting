import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import java.lang._
import java.io.File

/**
 * @author dhruv
 */
object modelGenerator {
  
  def main(args: Array[String]) {
    val sparkConf=new SparkConf().setAppName("KafkaNaiveBayes")
    sparkConf.setMaster("local[2]")
    val sc=new SparkContext(sparkConf)
    //val ssc = new StreamingContext(sc,Seconds(1))
    //ssc.checkpoint("checkpoint")

    val Array(pathname) =args
    val data=sc.textFile("/Users/dhruv/projects/WebsiteFingerPrinting/real_data/train2")
    
     val parsedData = data.map { line =>
       val parts = line.split(',')
       LabeledPoint(parts(0).toCharArray()(7).toDouble-48, Vectors.dense(parts(1).split(' ').map(_.toDouble).take(873)))
     }
    val model = NaiveBayes.train(parsedData, lambda = 1.0, modelType = "multinomial")
    model.save(sc, pathname)
    //ssc.start()
    //ssc.awaitTermination()
    



  }

}
