import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.xml.{ XML, NodeSeq }

object GraphXPageRankLinks {
  def main(args: Array[String]) {
    //Taking command line arguments to run the correct config 
    //1. Input HDFS File
    //2. Output HDFS File
    //3. Number of iterations (optional)
    val inputHadoopFile=args(0)
		val outputHadoopFile=args(1)
    val iterations = if (args.length > 2) args(2).toInt else 10
    //Setting Spark configuration and context
    val config = new SparkConf()
    config.setAppName("GraphXSparkPageRank")
    config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val context = new SparkContext(config)
    //Taking input
    val input = context.textFile(inputHadoopFile)
    //Converting titles to unique hashcodes
    def pageToID(title: String): VertexId = {
      title.toLowerCase.replace(" ", "").hashCode.toLong
    }

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

    //Vertices of the graph in graphx, mapped with IDs
    val vertices = wiki.map(tup => {
      val title = tup._1
      (pageToID(title), title)
    }).cache

    //Edges of the graph in graphx with initial 1.0 value for pagerank init
    //converted to a flatmap  
    val edges: RDD[Edge[Double]] = wiki.flatMap(tup => {
      val pageId = pageToID(tup._1)
      val info = tup._2.iterator
      info.map(x => Edge(pageId, pageToID(x), 1.0))
    }).cache

    //Construction of graph
    val graph = Graph(vertices, edges, "").subgraph(vpred = { (v, d) => d.nonEmpty }).cache
    val pageRankgraph = graph.staticPageRank(iterations).cache;
    val titleWithPageRank = graph.outerJoinVertices(pageRankgraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }
    //Giving order to the entries in the result and displaying top 100
    val finalResult = titleWithPageRank.vertices.top(100) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    }
    
    //giving out final output
    context.parallelize(finalResult).map(t => t._2._2 + ": " + t._2._1).saveAsTextFile(outputHadoopFile)
  }
}