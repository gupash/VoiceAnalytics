package org.infogain.voice.bean

case class AgentInfo(agentId: String, date: String, callRating: List[CallRating], BUName: String = "PUNE", geoLocation: String = "INDIA")