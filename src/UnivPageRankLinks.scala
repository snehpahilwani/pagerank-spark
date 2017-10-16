import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.xml.{XML, NodeSeq}

object UnivPageRankLinks{
	def main(args: Array[String]){
	  //Taking command line arguments to run the correct config 
    //1. Input HDFS File
    //2. Output HDFS File
	  //3. List of universities database file
    //4. Number of iterations
		val inputFile=args(0)
		val outputFile=args(1)
		val univDB=args(2)
		val iterations = if (args.length > 3) args(3).toInt else 10
		val conf=new SparkConf().setAppName("UnivPageRank")

		val context=new SparkContext(conf)

		val input=context.textFile(inputFile)
		val univList=context.textFile(univDB)

		//parse the wikipedia page data into a graph
		//generate vertex table

		val n=univList.count()
		
    //Converting titles to unique hashcodes
    def pageToID(title: String): VertexId = {
      title.toLowerCase.replace(" ", "").hashCode.toLong
    }

    
		def extractData(line: String):(String, Array[String])={
			val cols=line.split("\t")
			val (univ_data, content)=(cols(1),cols(3).replace("\\n","\n"))
			val id=new String(univ_data)
			val links=
				if(content=="\\N"){
					NodeSeq.Empty
				}else{
					try{
						XML.loadString(content) \\ "link" \ "target"
					}catch{
						case e: org.xml.sax.SAXParseException=>
							System.err.println("XML Parse Error")
						NodeSeq.Empty
					}
				}
			val outLinks=links.map(link=>new String(link.text)).toArray

			(id, outLinks)
		}
		val links=input.map(extractData _)


		val vertexTable:RDD[(VertexId, String)]=univList.map(line=>{
			(pageToID(line),line)
		}).cache

		val edgeTable:RDD[Edge[Double]]=links.flatMap(line=>{
			val pageId=pageToID(line._1)
			
			line._2.iterator.map(x=>Edge(pageId, pageToID(x), 1.0/n))
		}).cache
		
		val graph=Graph(vertexTable, edgeTable, "").subgraph(vpred={(v,d)=>d.nonEmpty}).cache
		val prGraph=graph.staticPageRank(iterations).cache
		val titleAndPrGraph=graph.outerJoinVertices(prGraph.vertices){
			(v, title, rank)=>(rank.getOrElse(0.0), title)
		}
		
		val result=titleAndPrGraph.vertices.top(100){
			Ordering.by((entry:(VertexId, (Double, String)))=>entry._2._1)
		}
		
		context.parallelize(result).map(t=>t._2._2+": "+t._2._1).saveAsTextFile(outputFile)
		result.foreach(t=>println(t._2._2+": "+t._2._1))
	}
}