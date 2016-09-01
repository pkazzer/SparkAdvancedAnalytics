//Import von Github

/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

import com.cloudera.datascience.common.XmlInputFormat

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}

import edu.umd.cloud9.collection.wikipedia.WikipediaPage
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage

import java.io.{FileOutputStream, PrintStream}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

//Daten von Hive und Stopwords holen
val DemoUser = "TestUser"
import org.apache.spark.sql._

val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
val hivesql =  hiveContext.sql("FROM demo_user_db.twitter_use_case1 SELECT text, key LIMIT 10000")
val rdd = hivesql.rdd
val raw = rdd.map {
 case Row(text: String, key: String) => (text,key)
}


val filename="/home/cloudera/AdvancedAnalytics/ch06-lsa/src/main/resources/stopwords.txt"
val getStops=scala.io.Source.fromFile(filename).getLines().toSet
val stopWords = sc.broadcast(getStops).value 

//Funktionen für Lemmatisierung definieren

def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
  }
def isOnlyLetters(str: String): Boolean = {
    // While loop for high performance
    var i = 0
    while (i < str.length) {
      if (!Character.isLetter(str.charAt(i))) {
        return false
      }
      i += 1
    }
    true
  }
def plainTextToLemmas(text: String, stopWords: Set[String], pipeline: StanfordCoreNLP)
    : Seq[String] = {
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences.asScala;
         token <- sentence.get(classOf[TokensAnnotation]).asScala) {
      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
        lemmas += lemma.toLowerCase
      }
    }
    lemmas
  }

//Lemmatisierung

val lemmatized: RDD[(String,Seq[String])] = raw.mapPartitions(iter => {
 val pipeline = createNLPPipeline()
 iter.map {case(text, key) => (key, plainTextToLemmas(text, stopWords, pipeline))}})

//Angepasstes Object ParseWikipedia definieren

object ParseWikipedia {
  /**
   * Returns a document-term matrix where each element is the TF-IDF of the row's document and
   * the column's term.
   */
  def documentTermMatrix(docs: RDD[(String, Seq[String])], stopWords: Set[String], numTerms: Int,
      sc: SparkContext): (RDD[Vector], Map[Int, String], Map[Long, String], Map[String, Double]) = {
    val docTermFreqs = docs.mapValues(terms => {
      val termFreqsInDoc = terms.foldLeft(new HashMap[String, Int]()) {
        (map, term) => map += term -> (map.getOrElse(term, 0) + 1)
      }
      termFreqsInDoc
    })

    docTermFreqs.cache()
    val docIds = docTermFreqs.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap()

    val docFreqs = documentFrequenciesDistributed(docTermFreqs.map(_._2), numTerms)
    println("Number of terms: " + docFreqs.size)
    //saveDocFreqs("docfreqs.tsv", docFreqs)

    val numDocs = docIds.size

    val idfs = inverseDocumentFrequencies(docFreqs, numDocs)

    // Maps terms to their indices in the vector
    val idTerms = idfs.keys.zipWithIndex.toMap
    val termIds = idTerms.map(_.swap)

    val bIdfs = sc.broadcast(idfs).value
    val bIdTerms = sc.broadcast(idTerms).value

     val vecs = docTermFreqs.map(_._2).map(termFreqs => {
      val docTotalTerms = termFreqs.values.sum
      val termScores = termFreqs.filter {
        case (term, freq) => bIdTerms.contains(term)
      }.map{
        case (term, freq) => (bIdTerms(term), bIdfs(term) * termFreqs(term) / docTotalTerms)
      }.toSeq
      Vectors.sparse(bIdTerms.size, termScores)
    })
    (vecs, termIds, docIds, idfs)
  }

  def documentFrequencies(docTermFreqs: RDD[HashMap[String, Int]]): HashMap[String, Int] = {
    val zero = new HashMap[String, Int]()
    def merge(dfs: HashMap[String, Int], tfs: HashMap[String, Int])
      : HashMap[String, Int] = {
      tfs.keySet.foreach { term =>
        dfs += term -> (dfs.getOrElse(term, 0) + 1)
      }
      dfs
    }
    def comb(dfs1: HashMap[String, Int], dfs2: HashMap[String, Int])
      : HashMap[String, Int] = {
      for ((term, count) <- dfs2) {
        dfs1 += term -> (dfs1.getOrElse(term, 0) + count)
      }
      dfs1
    }
    docTermFreqs.aggregate(zero)(merge, comb)
  }

  def documentFrequenciesDistributed(docTermFreqs: RDD[HashMap[String, Int]], numTerms: Int)
      : Array[(String, Int)] = {
    val docFreqs = docTermFreqs.flatMap(_.keySet).map((_, 1)).reduceByKey(_ + _, 15)
    val ordering = Ordering.by[(String, Int), Int](_._2)
    docFreqs.top(numTerms)(ordering)
  }

  def trimLeastFrequent(freqs: Map[String, Int], numToKeep: Int): Map[String, Int] = {
    freqs.toArray.sortBy(_._2).take(math.min(numToKeep, freqs.size)).toMap
  }

  def inverseDocumentFrequencies(docFreqs: Array[(String, Int)], numDocs: Int)
    : Map[String, Double] = {
    docFreqs.map{ case (term, count) => (term, math.log(numDocs.toDouble / count))}.toMap
  }
   def saveDocFreqs(path: String, docFreqs: Array[(String, Int)]) {
    val ps = new PrintStream(new FileOutputStream(path))
    for ((doc, freq) <- docFreqs) {
      ps.println(s"$doc\t$freq")
    }
    ps.close()
  }
}
 
//NumTerms Limit bestimmen und termDocMatrix, termIds, docIds, idfs ausgeben
val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer.mb","1000000") 
val numTerms = 50000
val (termDocMatrix, termIds, docIds, idfs) = ParseWikipedia.documentTermMatrix(lemmatized, stopWords, numTerms, sc)

//termDocMatrix.zip(docIDs).saveAsParquetFile("hdfs:///user/" + DemoUser + "/termDocMatrix")
////////////////////////////////////////////// kmeans 

//KMeans
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.util.MLUtils


def visualizationInR(rawData: RDD[Vector],k: Int): RDD[(Int,Int)] = {
    val data = rawData.cache()
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(5)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(data)
    val sample = data.map(datum =>
      (model.predict(datum),k)
    )
    //sample.saveAsTextFile("hdfs:///user/" + DemoUser + "/kmeans"+k)
    data.unpersist()
    (sample)
  }

//Lade Ergebnisstabelle
//import org.apache.spark.sql._
//val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
//val RepartAll = hiveContext.sql("FROM demo_user_db.twitter_use_case1 SELECT * LIMIT 10000").repartition(1)

//val termDocMatrix = MLUtils.loadVectors(sc, "hdfs:///user/" + DemoUser + "/termDocMatrix")
val parsedDataVal = termDocMatrix.map(_.toDense.values)
val vecdense2 = parsedDataVal.map(Vectors.dense(_)).cache()
for( k <- 10 to 30 by 10 ){
val clusterId = visualizationInR(vecdense2,k)
///Lade Zwischenergebnis ClusterId

val win2 = raw zip clusterId
val WinDF = win2.map({case((text: String, key: String), (clusterId, k))=>( text,key,clusterId,k)}).toDF("orgText","key","clusterId","k")
val newDF = WinDF.join(hivesql,"key")
if (k > 10){
newDF.write.mode("append").saveAsTable("demo_user_db.Twitter_ResultsZZZ")
}
else{
newDF.saveAsTable("demo_user_db.Twitter_ResultsZZZ")
}
}
// [755518498722443264,0,1,Having chronic migraines as well as stomach ulcers so you're unable to take aspirin is what actual hell is like🙃]
