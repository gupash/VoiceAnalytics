package org.infogain.voice.utils

import java.io.FileNotFoundException
import java.util.Properties

import com.mongodb.casbah.MongoClient

object PropertyLoader {

  val propHandle = loadProperties("App.properties")
  val dbName = "com.stratio.datasource.mongodb"

  def loadProperties(filename: String) = {
    val filterKeyWords = Option(getClass.getClassLoader.getResourceAsStream(filename))
    filterKeyWords match {
      case Some(file) =>
        val prop = new Properties()
        prop.load(file)
        file.close()
        prop
      case None => throw new FileNotFoundException(s"Property File: $filename is not found")
    }
  }

  def getProp(propertyName: String): String = {
    propHandle.getProperty(propertyName)
  }

  def getDbHost(): String = {
    getProp("HOST")
  }

  def getIP(): String = {
    getProp("IP")
  }

  def getDb(): String = {
    getProp("DATABASE")
  }

  def getCollection(): String = {
    getProp("COLLECTION")
  }

  def getHdfsUri(): String = {
    getProp("HDFS_URI")
  }

  def getSelectedCollection() = {
    val mongoClient = MongoClient(getDbHost, 27017)
    val db = mongoClient(getDb)
    db(getCollection)
  }

  def getOfensiveWords() = {
    getProp("OFFENSIVE_WORDS")
  }
}
