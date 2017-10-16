import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.xml.{ XML, NodeSeq }

object SparkPageRank {
  def main(args: Array[String]) {
    //Taking command line arguments to run the correct config 
    //1. Input HDFS File
    //2. Output HDFS File
    //3. Number of iterations (optional)
    val inputHadoopFile = args(0)
    val outputHadoopFile = args(1)
    val iterations = if (args.length > 2) args(2).toInt else 10
    val config = new SparkConf()
    config.setAppName("SparkPageRank")
    config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val context = new SparkContext(config)
    val input = context.textFile(inputHadoopFile)

    // Arranging the pages into graph structure to implement PageRank
    var wiki = input.map(line => {
      val cols = line.split("\t")
      val (wiki_title, neighbor_data) = (cols(0), cols(3).replace("\\n", "\n"))
      val extracted_links =
        if (neighbor_data == "\\N") {
          NodeSeq.Empty
        } else {
          try {
            XML.loadString(neighbor_data) \\ "link" \ "target"

          } catch {
            case e: org.xml.sax.SAXParseException =>
              System.err.println("XML Parse Error")
              NodeSeq.Empty
          }
        }
      val outLinks = extracted_links.map(link => new String(link.text)).toArray
      val id = new String(wiki_title)
      (id, outLinks)
    }).cache
    var pageRanks = wiki.mapValues(v => 1.0)
    for (i <- 1 to iterations) {
      val contribs = wiki.join(pageRanks).values.flatMap {
        case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
          pageRanks = contribs.reduceByKey(_+_).mapValues (0.15 + 0.85 * _)
      
      val output = pageRanks.takeOrdered(100)(Ordering[Double].reverse.on(x => x._2))
      context.parallelize(output).saveAsTextFile(outputHadoopFile)
      pageRanks.collect().foreach(tup => println(tup._1 + "    " + tup._2 + "."))
      context.stop()
    }
  }
}