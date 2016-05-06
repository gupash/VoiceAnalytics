package org.infogain.voice.sentimentAnalysis

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object SentimentAnalysis {

  val nlpProps = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    props.setProperty("ssplit.eolonly", "true")
    props
  }

  @transient lazy val pipeline = new StanfordCoreNLP(nlpProps)

  def detectSentiment(message: String) = {

    val annotation = pipeline.process(message)
    var sentiments: ListBuffer[Double] = ListBuffer()
    var sizes: ListBuffer[Int] = ListBuffer()

    var longest = 0
    var mainSentiment = 0

    for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
      val partText = sentence.toString

      if (partText.length() > longest) {
        mainSentiment = sentiment
        longest = partText.length()
      }

      sentiments += sentiment.toDouble
      sizes += partText.length
    }

    val weightedSentiments = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
    var weightedSentiment = weightedSentiments.sum / (sizes.fold(0)(_ + _))

    if (sentiments.size == 0) {
      mainSentiment = -1
      weightedSentiment = -1
    }

    /*
     0 -> very negative
     1 -> negative
     2 -> neutral
     3 -> positive
     4 -> very positive
     */
    weightedSentiment match {
      case s if s <= 0.0 => ("NOT_UNDERSTOOD", 0)
      case s if s < 1.0 => ("VERY_NEGATIVE", 1)
      case s if s < 2.0 => ("NEGATIVE", 2)
      case s if s < 3.0 => ("NEUTRAL", 3)
      case s if s < 4.0 => ("POSITIVE", 4)
      case s if s < 5.0 => ("VERY_POSITIVE", 5)
      case s if s > 5.0 => ("NOT_UNDERSTOOD", 0)
    }

  }

  /*trait SENTIMENT_TYPE

  case object VERY_NEGATIVE extends SENTIMENT_TYPE
  case object NEGATIVE extends SENTIMENT_TYPE
  case object NEUTRAL extends SENTIMENT_TYPE
  case object POSITIVE extends SENTIMENT_TYPE
  case object VERY_POSITIVE extends SENTIMENT_TYPE
  case object NOT_UNDERSTOOD extends SENTIMENT_TYPE*/
}