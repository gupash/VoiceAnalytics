package org.infogain.voice.dao

import com.mongodb.casbah.{MongoCollection, MongoConnection}
import com.mongodb.casbah.commons.MongoDBObject
import org.infogain.voice.bean._

/** @author ankur
  *         Desc: This class contaig many queries for DB
  */


object DbUtils {

  private val HOST = "localhost"
  private val PORT = 27017
  private val DATABASE = "agentCallRecord"


  /** @author ankur
    *         return : MongoConnection
    *         Desc : Creating connection and returing Instance of the same
    */

  def getConnection(): MongoConnection = {
    val mongoConn = MongoConnection(HOST, PORT)
    return mongoConn;
  }

  /** @author ankur
    *         return : MongoCollection
    *         Desc   : Returning Instance of the Collection
    */
  def getCollection(collection: String): MongoCollection = {
    var conn = getConnection();
    return conn(DATABASE)(collection)
  }


  /** @author ankur
    *         return : Non
    *         Desc   : Used to Insert and/or Update
    *         conditionally
    */
  def insertCallRecord(obj: CallDetails, mongoColl: String) {
    val add_data = MongoDBObject("agentId" -> obj.agentId, "date" -> obj.date)
    println("Search Query :" + add_data)
    val collection = getCollection(mongoColl)
    if (collection.find(add_data).size <= 0) {
      val add_data = MongoDBObject("agentId" -> obj.agentId, "firstName" -> obj.firstName, "lastName" -> obj.lastName, "agentId" -> obj.agentId, "date" -> obj.date, "callRating" -> obj.callRating)
      collection.insert(add_data);
    }
    else {
      val condition = MongoDBObject("agentId" -> obj.agentId, "date" -> obj.date)
      val add_data = MongoDBObject("$addToSet" -> MongoDBObject("callRating" -> obj.callRating.head))
      println("Update Query Condition::" + condition)
      println("Update Query Data  ::" + add_data)
      collection.update(condition, add_data);
    }
  }

  /** @author ankur
    *         return : Non
    *         Desc   : Generating conditional Result
    *         Condition : Total Rating of All Agents within TimeRange
    */


  def getResultForTotalCallRatingForAllAgentLastOneHours(coll: String, fromDate: String, toDate: String) {
    val collection = getCollection(coll)
    val result = collection.aggregate(
      List(
        MongoDBObject("$unwind" -> "$callRating"),
        MongoDBObject("$match" ->
          MongoDBObject("callRating.startTime" ->
          MongoDBObject("$gte" -> fromDate, "$lte" -> toDate))),
        MongoDBObject("$group" ->
          MongoDBObject("_id" -> "$agentId", "totalScore" ->
            MongoDBObject("$sum" -> "$callRating.totalScore")))))

    val itr = result.results.iterator
    itr.foreach(println)
  }


  /** @author ankur
    *         return : Non
    *         Desc   : Generating conditional Result
    *         Condition : Total Rating of One Agents within TimeRange
    */

  def getResultForTotalCallRatingForOneAgentLastOneHours(agentId: String, coll: String, fromDate: String, toDate: String) {
    val collection = getCollection(coll)
    val result = collection.aggregate(
      List(
        MongoDBObject("$match" -> MongoDBObject("agentId" -> agentId)),
        MongoDBObject("$unwind" -> "$callRating"),
        MongoDBObject("$match" ->
          MongoDBObject("callRating.startTime" ->
            MongoDBObject("$gte" -> fromDate, "$lte" -> toDate))),
        MongoDBObject("$group" ->
          MongoDBObject("_id" -> "$agentId", "totalScore" ->
            MongoDBObject("$sum" -> "$callRating.totalScore")))))

    val itr = result.results.iterator
    itr.foreach(println)
  }


  /** @author ankur
    *         return : Non
    *         Desc   : Releasing DB Resource
    */
  def closeConnection(conn: MongoConnection) {
    conn.close
  }
}