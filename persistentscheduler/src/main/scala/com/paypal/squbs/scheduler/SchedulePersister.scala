package com.paypal.squbs.scheduler

import akka.Done
import akka.util.ByteString
import scheduledevents.{ScheduledBuckets, ScheduledEvent, ScheduledEvents}

import scala.collection.immutable.SortedSet
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class SchedulePersister(val rcmw: AsyncRCMW, val clusterName: String, val nodeId: String,
                        val bucketSize: Long, val clock: Clock = new WallClock)
                       (implicit val ec: ExecutionContext) {

  val bucketSetKey = s"${clusterName}_${nodeId}_buckets"

  private var bucketSetDirty = false
  private var bucketSet = SortedSet.empty[Long]

  def init(): Future[Done] = rcmw.read(bucketSetKey).map {
    case Some(bytes) =>
      bucketSet = bytesToBuckets(bytes).to[SortedSet]
      Done
    case None =>
      bucketSet = SortedSet.empty[Long]
      Done
  }

  def writeSchedule(event: ScheduledEvent): Future[CreatedOrUpdated[Long]] = {
    val eventBucket = bucket(event.msFromEpoch)

    val actionTaken = rcmw(bucketKey(eventBucket),
      { _ =>
        val events = Seq(event)
        (eventsToBytes(events), events)
      },
      (_, b) => appendEventsToBytes(b, event))

    actionTaken.map {
      case _: Created[_] => Created(eventBucket)
      case _: Updated[_] => Updated(eventBucket)
    }

    /*
    After we are done creating a new bucket we also need to ensure the newly created bucket is added to the list
    of buckets.
    Problem: Writing multiple keys to a key-value store is non-atomic. There is a slim possibility the new bucket
    gets created, and then the write of the bucket to the ScheduledBuckets doc fails. Since we depend on this list
    for searching the buckets, missing this could cause a bucket to be left in the key-value store for good.

    No solution just yet. We cannot even change the ordering as only once the bucket is created we'd know of
    the creation (as opposed to adding events to the list)
     */
//    actionTaken.flatMap { createdOrUpdated =>
//      bucketSet += eventBucket
//      checkWriteBucketSet(createdOrUpdated)
//    }
  }

  def addBucketToIndex(bucketId: Long): Unit = {
    bucketSet += bucketId
    bucketSetDirty = true
  }

  def removeBucketFromIndex(bucketId: Long): Unit = {
    bucketSet -= bucketId
    bucketSetDirty = true
  }

  var counter = 0
  private def writeBucketSet(): Future[Done] = {
    val myCount = counter
    counter = counter + 1
    println(s"*** BUCKET SET ($myCount): Writing!")
    rcmw(bucketSetKey,
      _ => bucketsToBytes,
      { (_, storedData) =>
        bucketSet ++= bytesToBuckets(storedData)
        bucketsToBytes
      }
    ).map { _ =>
      println(s"*** BUCKET SET ($myCount): Write successful")
      Done
    }
      .recoverWith {
        case e =>
          println(s"*** BUCKET SET ($myCount): Write failed: $e")
          Future.failed(e)
      }
  }

  def removeBucket(bucket: Long): Future[Done] = rcmw.delete(bucketKey(bucket))

  def removeIndividualEvent(bucketId: Long, event: ScheduledEvent): Future[Seq[ScheduledEvent]] = {
    val x = rcmw(bucketKey(bucketId),
      _ => (ByteString.empty, Seq.empty[ScheduledEvent]),
      { (_, byteString) =>
        val events = bytesToEvents(byteString).filterNot(_ == event)
        (eventsToBytes(events), events)
      }
    )
      x.flatMap {
      case Created(_) =>
        Future.failed(new UnsupportedOperationException("Remove should never result in a bucket created!"))
      case Updated(events) =>
        Future.successful(events)
    }
  }

  def nextBucketReadWait: FiniteDuration = {
    val currentTime = clock.currentTimeMillis
    val nextReadTime = bucket(currentTime) * bucketSize + (bucketSize / 2)
    val waitTime = nextReadTime - currentTime
    if (waitTime < 5) Duration.Zero else waitTime.millis
  }

  def readSchedule(bucket: Long): Future[Seq[ScheduledEvent]] = {
    rcmw.read(bucketKey(bucket)).map {
      case Some(bytes) => bytesToEvents(bytes)
      case None => Seq.empty
    }
  }

  def nextBucketEvents: (Long, Future[Seq[ScheduledEvent]]) = {
    val bucketId = nextBucket
    val result = readSchedule(bucketId)
    (bucketId, result)
  }

  def pastDueBuckets: SortedSet[Long] = {
    val cb = currentBucket
    bucketSet.takeWhile(_ <= cb)
  }

  def bucket(msFromEpoch: Long): Long = msFromEpoch / bucketSize

  private def currentBucket = bucket(clock.currentTimeMillis)

  private def nextBucket: Long = currentBucket + 1

  private[scheduler] def bucketKey(bucket: Long) = s"${clusterName}_${nodeId}_$bucket"

  private def appendEventsToBytes(bytes: ByteString, event: ScheduledEvent): (ByteString, Seq[ScheduledEvent]) = {
    val events = bytesToEvents(bytes) :+ event
    (eventsToBytes(events), events)
  }

  private def eventsToBytes(eventList: Seq[ScheduledEvent]): ByteString =
    ByteString(ScheduledEvents(eventList).toByteArray)

  private def bytesToEvents(bytes: ByteString): Seq[ScheduledEvent] = ScheduledEvents.parseFrom(bytes.toArray).events

  private def bucketsToBytes: (ByteString, Seq[Long]) = {
    val buckets = bucketSet.toSeq
    (ByteString(ScheduledBuckets(buckets).toByteArray), buckets)
  }

  private def bytesToBuckets(bytes: ByteString): Seq[Long] =
    ScheduledBuckets.parseFrom(bytes.toArray).bucketIds
}
