package com.paypal.squbs.scheduler

import akka.Done
import akka.actor.ActorSystem
import akka.event.{LogSource, Logging}
import akka.util.ByteString
import com.ebay.inc.platform.mayfly.{MayflyContext, MayflyOperationStatus => OpsStatus}
import com.paypal.squbs.rocksqubs.mayfly.MayflyClient

import scala.concurrent.{ExecutionContext, Future}

object MayflyRCMW {
  class PersistenceException(message: String) extends Exception(message)

  sealed trait RetryableException extends Exception

  class DuplicateKeyException(val ctx: MayflyContext) extends RetryableException

  class OptimisticWriteException extends RetryableException

  implicit val logSource: LogSource[MayflyRCMW] = (_: MayflyRCMW) => ""
}

class MayflyRCMW(mayflyClient: MayflyClient)(implicit val system: ActorSystem)
  extends AsyncRCMWEngine[MayflyContext, MayflyRCMW.RetryableException] {

  import MayflyRCMW._

  implicit val ec: ExecutionContext = system.dispatcher

  val log = Logging(system, this)

  def createPersist(key: String, value: ByteString): Future[Done] = {
    if (value.nonEmpty) {
      mayflyClient.create(key, value).flatMap { response =>
        if (response.status == OpsStatus.MAYFLY_SUCCESS) {
          log.debug("Created key {} from mayfly. Value size {} bytes.", key, value.size)
          Future.successful(Done)
        } else if (response.status == OpsStatus.DupKey) {
          log.debug("Duplicate Key: Creating key {} from mayfly. Value size {} bytes.", key, value.size)
          Future.failed(new DuplicateKeyException(response.context))
        } else {
          val ex = new PersistenceException(s"Abnormal Mayfly operation status on create: ${response.status}")
          log.error(ex, "Mayfly create operation response {}. Value size {} bytes", response.status, value.size)
          Future.failed(ex)
        }
      }
    } else {
      Future.successful(Done)
    }
  }

  def readPersist(key: String): Future[(Option[ByteString], MayflyContext)] = {
    mayflyClient.read(key).flatMap { response =>
      val ctx = response.context
      if (response.status == OpsStatus.MAYFLY_SUCCESS) {
        val value = response.data
        log.debug("Read key {} from mayfly. Value size {} bytes", key, value.size)
        Future.successful(Some(value), ctx)
      } else if (response.status == OpsStatus.NoKey || response.status == OpsStatus.DataExpired) {
        log.debug("Reading: No such key {}", key)
        Future.successful(None, ctx)
      } else {
        val ex = new PersistenceException(s"Abnormal Mayfly operation status on read: ${response.status}")
        log.error(ex, "Mayfly read operation response {}", response.status)
        Future.failed(ex)
      }
    }
  }

  def updatePersist(key: String, value: ByteString, ctx: MayflyContext): Future[Done] = {
    mayflyClient.conditionalUpdate(ctx, value, 259000L).flatMap { response =>
      if (response.status == OpsStatus.MAYFLY_SUCCESS) {
        log.debug("Updated key {} on mayfly. Value size {} bytes.", key, value.size)
        Future.successful(Done)
      } else if (response.status == OpsStatus.VersionConflict) {
        log.debug("Optimistic exception version conflict for key {} on mayfly. Value size {} bytes.", key, value.size)
        Future.failed(new OptimisticWriteException)
      } else {
        val ex = new PersistenceException(s"Abnormal Mayfly operation status on update: ${response.status}")
        log.error(ex, "Mayfly update operation response {}. Value size {} bytes.", response.status, value.size)
        Future.failed(ex)
      }
    }
  }

  override def deletePersist(key: String): Future[Done] = {
    mayflyClient.destroy(key).flatMap { response =>
      if (response.status == OpsStatus.MAYFLY_SUCCESS) {
        log.debug(s"Deleted key {} from mayfly", key)
        Future.successful(Done)
      } else {
        val ex = new PersistenceException(s"Abnormal Mayfly operation status on delete: ${response.status}")
        log.error(ex, "Mayfly delete operation response {}", response.status)
        Future.failed(ex)
      }
    }
  }
}

class MayflyRCMWFactory extends RCMWFactory {
  override def create(implicit system: ActorSystem): AsyncRCMW = {
    val mayflyName = system.settings.config.getString("mayfly-rcmw.mayfly-name")
    new MayflyRCMW(MayflyClient(mayflyName))
  }
}

