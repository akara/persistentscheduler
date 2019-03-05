package com.paypal.squbs.scheduler

import java.util.concurrent.ConcurrentHashMap

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.util.ByteString
import com.paypal.squbs.scheduler.MayflyRCMW.RetryableException

import scala.concurrent.{ExecutionContext, Future}

object MapBasedRCMW {
  val dataStore = new ConcurrentHashMap[String, ByteString]
}

class MapBasedRCMWFactory extends RCMWFactory {
  override def create(implicit system: ActorSystem): AsyncRCMW = new MapBasedRCMW()(system.dispatcher)
}

class MapBasedRCMW(implicit val ec: ExecutionContext) extends AsyncRCMWEngine[NotUsed, RetryableException] {
  import MapBasedRCMW._


  override def createPersist(key: String, value: ByteString): Future[Done] = {
    dataStore.put(key, value)
    Future.successful(Done)
  }

  override def readPersist(key: String): Future[(Option[ByteString], NotUsed)] =
    Future.successful(Option(dataStore.get(key)), NotUsed)

  override def updatePersist(key: String, value: ByteString, ctx: NotUsed): Future[Done] = {
    if (dataStore.containsKey(key)) createPersist(key, value)
    else Future.failed(new NoSuchElementException(s"No previous value for $key"))
  }

  override def deletePersist(key: String): Future[Done] = {
    Option(dataStore.remove(key)) match {
      case Some(_) => Future.successful(Done)
      case None => Future.failed(new NoSuchElementException(s"No value for $key"))
    }
  }

}
