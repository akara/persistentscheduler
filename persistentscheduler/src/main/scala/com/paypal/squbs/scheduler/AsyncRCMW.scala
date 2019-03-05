package com.paypal.squbs.scheduler

import akka.Done
import akka.actor.ActorSystem
import akka.util.ByteString

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.{ClassTag, classTag}

sealed trait CreatedOrUpdated[T]
case class Created[T](content: T) extends CreatedOrUpdated[T]
case class Updated[T](content: T) extends CreatedOrUpdated[T]

/**
  * Base trait providing the API for read/create/modify/write usage.
  * The API allows users to specify the synchronous mutation functions for simplicity.
  */
trait AsyncRCMW {

  def apply[D](key: String,
            createData: String => (ByteString, D),
            modData: (String, ByteString) => (ByteString, D),
            maxRetry: Int = 3): Future[CreatedOrUpdated[D]]

  def read(key: String): Future[Option[ByteString]]

  def delete(key: String): Future[Done]
}

/**
  * Logic for asynchronous read/create/modify/write based on futures.
  * This base class provides the SPI interfaces for persistence mechanism and the core logic for RCMW.
  *
  * @tparam C The context type, which can be `NotUsed` for data stores not supporting conditional updates.
  * @tparam R The exception type forcing the RCMW to retry.
  */
abstract class AsyncRCMWEngine[C, R <: Exception : ClassTag] extends AsyncRCMW {

  implicit val ec: ExecutionContext

  def createPersist(key: String, value: ByteString): Future[Done]

  def readPersist(key: String): Future[(Option[ByteString], C)]

  def updatePersist(key: String, value: ByteString, ctx: C): Future[Done]

  def deletePersist(key: String): Future[Done]

  final def apply[D](key: String,
            createData: String => (ByteString, D),
            modData: (String, ByteString) => (ByteString, D),
            maxRetry: Int = 3): Future[CreatedOrUpdated[D]] = {

    @volatile var retries = 0

    object Retryable {
      def apply(t: Throwable): Boolean = classTag[R].runtimeClass.isInstance(t) && retries <= maxRetry
      def unapply(t: R): Option[R] = if (apply(t)) Some(t) else None
    }

    def rcmw(): Future[CreatedOrUpdated[D]] =
      readPersist(key)
        .flatMap {
          case (None, _) =>
            val (bytes, content) = createData(key)
            createPersist(key, bytes).map(_ => Created(content))
          case (Some(data), c) =>
            val (bytes, content) = modData(key, data)
            updatePersist(key, bytes, c).map(_ => Updated(content))
        }
        .recoverWith {
          case Retryable(_) => retries += 1; rcmw()
          case t => Future.failed(t)
        }

    rcmw()
  }

  final def read(key: String): Future[Option[ByteString]] = readPersist(key).map { case (option, _) => option }

  final def delete(key: String): Future[Done] = deletePersist(key)
}

trait RCMWFactory {
  def create(implicit system: ActorSystem): AsyncRCMW
}

