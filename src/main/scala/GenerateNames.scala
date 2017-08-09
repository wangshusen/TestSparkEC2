import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.Random
object GenerateNames {
    val SPARK_MASTER = "spark://ec2-52-35-41-62.us-west-2.compute.amazonaws.com:7077"
    val HDFS = "hdfs://ec2-52-35-41-62.us-west-2.compute.amazonaws.com:9000/"
    val outputDir = HDFS + "/output/part"
    def main(args: Array[String]) {
        val conf = new SparkConf()
        .setMaster(SPARK_MASTER)
        .setAppName("GenerateNames")
        val sc = new SparkContext(conf)
        for (partition <- 0 to 3) {
            val data = Seq.fill(1000000)(Random.alphanumeric.take(5).mkString)
            sc.parallelize(data, 1).saveAsTextFile(outputDir + "_" + partition)
        }
    }
}