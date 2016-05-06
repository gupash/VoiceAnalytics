package org.infogain.voice.processor

import java.io.IOException
import java.net.URI
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.query.Imports._
import org.apache.hadoop.hdfs.client.HdfsAdmin
import org.apache.hadoop.io.{DoubleWritable, Text}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.infogain.voice.bean.{AgentInfo, CallRating}
import org.infogain.voice.customInputFormat.WavFileInputFormat
import org.infogain.voice.sentimentAnalysis.SentimentAnalysis._
import org.infogain.voice.utils.PropertyLoader._

import scala.util.Random

object FileProcessor {

  val hadoopConf = new org.apache.hadoop.conf.Configuration()
  val HDFS_URI: String = getHdfsUri()
  val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(HDFS_URI), hadoopConf)
  val options = Map("host" -> s"$getDbHost:$getIP", "database" -> getDb(), "collection" -> getCollection())
  val admin = new HdfsAdmin(URI.create(getHdfsUri()), hadoopConf)

  val sparkConf = new SparkConf().setAppName("File Processor")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]) {

    fileProcessing(args(0))

    def generateScore(fileSize: Double): Int = {
      fileSize match {
        case i if i > 0 && i <= 500 => 1
        case i if i > 500 && i <= 1000 => 2
        case _ => 3
      }
    }

    def fileProcessing(inputPath: String) = {

      import sqlContext.implicits._

      try {
        val arrStr = inputPath.split("/")
        val fileName = Option(arrStr(arrStr.length - 1).trim)

        fileName.foreach(x => {
          val agentInfoData = getAgentInfo(inputPath, x)
          val mongoColl = getSelectedCollection()
          val query = MongoDBObject("agentId" -> agentInfoData.agentId)
          val findAgent = mongoColl.findOne(query)

          val date = findAgent.map(x => x.toMap.get("date").asInstanceOf[String]).getOrElse("No value")

          if (date == agentInfoData.date) {

            val agentCallRatingData = agentInfoData.callRating.head
            val callRatingMongoObject = MongoDBObject("audioId" -> agentCallRatingData.audioId, "startTime" -> agentCallRatingData.startTime,
              "endTime" -> agentCallRatingData.endTime, "lengthScore" -> agentCallRatingData.lengthScore, "sentiment" -> agentCallRatingData.sentiment,
              "sentimentScore" -> agentCallRatingData.sentimentScore, "offensiveScore" -> agentCallRatingData.offensiveScore)

            val update = $push("callRating" -> callRatingMongoObject)
            mongoColl.update(query, update)

          } else {
            sc.parallelize(Array(agentInfoData)).repartition(1).toDF().write.format(dbName).mode(SaveMode.Append).options(options).save
          }
        })
      } catch {
        case e: IOException => e.printStackTrace()
      }
      finally {
        sc.stop
      }
    }

    def getAgentInfo(inputPath: String, fileName: String): AgentInfo = {

      val agentID = getAgentId(fileName)

      val file = sc.newAPIHadoopFile(HDFS_URI + inputPath, classOf[WavFileInputFormat], classOf[DoubleWritable], classOf[Text], hadoopConf)

      val processedFile = file.map(x => (x._1.get, x._2.toString)).first

      println(s"******** Hypothesis *********** \n${processedFile._2}")

      val sentiment = detectSentiment(processedFile._2)

      val randomID = math.abs(Seq.fill(5)(Random.nextInt) head)

      val df = new SimpleDateFormat("yyyy-MM-dd")
      val currDate = df.format(new Date)

      val startTime = new Timestamp(new Date().getTime)
      val endTime = startTime.getTime + processedFile._1 * 1000

      val offensiveScore = genrateOffensiveScore(getOfensiveWords.toLowerCase.split(","), processedFile._2.toLowerCase)

      val callRatingData = CallRating(randomID.toString, startTime.toString, new Timestamp(endTime.toLong).toString, generateScore(processedFile._1), sentiment._1, sentiment._2, offensiveScore)
      val agentInfo = AgentInfo(agentID, currDate, List(callRatingData))
      print(agentInfo)
      agentInfo
    }

    def getAgentId(fileName: String): String = {
      if (fileName.contains("_")) fileName.split("_", 2)(1).split("\\.")(0).trim else ""
    }

    def genrateOffensiveScore(offensiveWords: Array[String], fileText: String) = {
      offensiveWords.foldLeft(0)((acc, crr) => if (fileText.contains(crr)) acc + 1 else acc)
    }
  }
}