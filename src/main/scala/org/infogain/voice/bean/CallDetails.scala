package org.infogain.voice.bean

/**
  *
  * @author ankur
  *         Desc: This as dao call associated and sink with
  *         DB and collection
  */

class CallDetails(Fname: String, Lname: String, aId: String, dt: String, cr: List[scala.collection.mutable.Map[String, Any]]) {
  var firstName = Fname
  var lastName = Lname
  var agentId = aId
  var date = dt
  var callRating = cr
}