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
object KafkaConsumer {
  
  def main(args: Array[String]) {
    val sparkConf=new SparkConf().setAppName("KafkaNaiveBayes")
    sparkConf.setMaster("local[2]")
    val sc=new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(1))
    ssc.checkpoint("checkpoint")

    val Array(zkQuorum, group, topics, numThreads, modelname) = args
    val topicMap=topics.split(",").map((_,numThreads.toInt)).toMap
    val model = NaiveBayesModel.load(sc, modelname)

 
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group,  topicMap).map(_._2)
    val parsedData = lines.window(Seconds(10)).map { line =>
       val parts = line.split(',')
       LabeledPoint(parts(0).toCharArray()(7).toDouble-48, Vectors.dense(parts(1).split(' ').map(_.toDouble).take(873)))
     }

//preditc
     val predictionAndLabel = parsedData.map(p => (model.predict(p.features), p.label))
//print predictions and actual labels
    
    var count=0.0
    var correct=0.0
    var wrong=0.0

     predictionAndLabel.foreach(x=>{
         x.collect().foreach(x=>{
            println(x._1+"  "+x._2)
            count=count+1.0
            if(x._1==x._2)
                correct=correct+1.0
            else
                wrong=wrong+1.0
         })
        if(count>0.0)
        {
         println("Accuracy: "+(correct/count))
         println("Error: "+(wrong/count))
     }
     })

    ssc.start()
    ssc.awaitTermination()
    



  }

}
