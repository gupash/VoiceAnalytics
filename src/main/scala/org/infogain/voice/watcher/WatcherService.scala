package org.infogain.voice.watcher

import java.net.URI

import org.apache.hadoop.hdfs.client.HdfsAdmin
import org.apache.hadoop.hdfs.inotify.Event.{EventType, RenameEvent}
import org.infogain.voice.utils.PropertyLoader._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.sys.process._

object WatcherService {

  val hadoopConf = new org.apache.hadoop.conf.Configuration()
  val admin = new HdfsAdmin(URI.create(getHdfsUri), hadoopConf)
  val eventStream = admin.getInotifyEventStream

  def main(args: Array[String]) {

    println("Registered Watcher Service ...")

    while (true) {
      val batch = eventStream.take
      for (event <- batch.getEvents) {

        event.getEventType match {

          case EventType.RENAME => {
            val renameEvent = event.asInstanceOf[RenameEvent]
            if (renameEvent.getDstPath.contains(".wav")) {
              Future {
                println(s"New File: ${renameEvent.getDstPath}")
                s"spark-submit --master yarn-cluster /Users/ashish/IdeaProjects/VoiceAnalytics/classes/artifacts/VoiceAnalytics/VoiceAnalytics.jar ${renameEvent.getDstPath}" !
              }
            }
          }
          case _ => None
        }
      }
    }
  }
}