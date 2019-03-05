package com.paypal.squbs.scheduler

import akka.actor.{Actor, ActorLogging, Stash}
import scheduledevents.ScheduledEvent

import scala.collection.mutable

class ScheduleCacheActor(persister: SchedulePersister) extends Actor with Stash with ActorLogging {

  case class Bucket(events: mutable.Set[ScheduledEvent], var dirty: Boolean = false, var lastTouched: Long)

  val cachedBuckets = mutable.LongMap.empty[Bucket]
  override def receive: Receive = {

    case event: ScheduledEvent =>
      val currentTime = System.currentTimeMillis
      val bucketId = persister.bucket(event.msFromEpoch)
      cachedBuckets.get(bucketId) match {
        case Some(bucket) =>
          bucket.events += event
          bucket.dirty = true
          bucket.lastTouched = currentTime
        case None =>
          cachedBuckets += bucketId -> Bucket(mutable.Set(event), dirty = true, currentTime)
      }
      // TODO: Scan & persist, but skip if already running.
  }

}
