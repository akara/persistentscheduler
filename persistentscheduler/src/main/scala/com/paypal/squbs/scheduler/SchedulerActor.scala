package com.paypal.squbs.scheduler

import akka.actor.{Actor, ActorLogging, ActorRef, Stash, Status, Timers}
import akka.pattern._
import com.codahale.metrics.MetricRegistry
import org.squbs.metrics.MetricsExtension
import scheduledevents.ScheduledEvent
import org.squbs.util.ConfigUtil._

import scala.collection.immutable.LongMap
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

case class RegisterForEvents(targetName: String, target: ActorRef)
case class ScheduleEvent(msFromEpoch: Long, target: String, eventKey: String)

object SchedulerActor {
  case object FetchTimer
  case object Initialized
  case object FetchFirstBucket
  case object FetchBucket
  case class BucketEvents(bucketId: Long, events: Seq[ScheduledEvent])
  case class RemoveLiveBuckets(buckets: Iterable[Long])
  case class RemoveStoredBucket(bucketId: Long, live: Boolean = true)
  case class RemoveBucketFromIndex(bucketId: Long)
}

class SchedulerActor extends Actor with Timers with Stash with ActorLogging {

  import SchedulerActor._
  import context.{dispatcher, system}

  val clock: Clock = system.settings.config.getOption[String]("persistent-scheduler.clock-provider")
    .map(Class.forName(_).asSubclass(classOf[Clock]).newInstance)
    .getOrElse(new WallClock)

  val nodeIdProviderClass: String = system.settings.config.getString("persistent-scheduler.node-id-provider")
  val nodeIdProvider: NodeIdProvider = Class.forName(nodeIdProviderClass).asSubclass(classOf[NodeIdProvider]).newInstance
  val rcmwFactoryClass: String = system.settings.config.getString("persistent-scheduler.rcmw-factory ")
  val rcmwFactory: RCMWFactory = Class.forName(rcmwFactoryClass).asSubclass(classOf[RCMWFactory]).newInstance

  val clusterName: String = nodeIdProvider.clusterName
  val nodeId: String = nodeIdProvider.nodeId
  val bucketSize: Long = system.settings.config.getDuration("persistent-scheduler.bucket-size").toMillis
  val timeBuffer: Long = system.settings.config.getDuration("persistent-scheduler.time-buffer").toMillis
  val maxPastDue: Long = system.settings.config
    .getOption[FiniteDuration]("persistent-scheduler.max-recovery")
    .map(_.toMillis).getOrElse(-1L)



  private var targets = Map.empty[String, ActorRef]

  private var pendingTargets = Map.empty[String, mutable.Buffer[ScheduledEvent]]

  private var liveBuckets = LongMap.empty[mutable.Set[ScheduledEvent]]

  private var lastScheduledBucket = -1L

  // Following are for metrics
  val metricsDomain = MetricsExtension(system).Domain + ".scheduler"
  val metrics = MetricsExtension(system).metrics
  val scheduled = MetricRegistry.name(metricsDomain, "scheduled-events")
  val delivered = MetricRegistry.name(metricsDomain, "delivered-events")
  val pendingCount = MetricRegistry.name(metricsDomain, "events-pending-registration")
  val persisted = MetricRegistry.name(metricsDomain, "persisted-events")
  val bucketEventCount = MetricRegistry.name(metricsDomain, "events-per-bucket")
  val retrieved = MetricRegistry.name(metricsDomain, "retrieved-events")


  val persister = new SchedulePersister(rcmwFactory.create, clusterName, nodeId, bucketSize, clock)
  persister.init().map(_ => Initialized) pipeTo self

  context.become(initializing, discardOld = false)

  def initializing: Receive = {
    case Initialized =>
      unstashAll()
      context.unbecome()
    case _ => stash()
  }

  def receive: Receive = {

    // Sent by event receiver to register to receive events.
    case RegisterForEvents(targetName, targetActor) =>
      targets += targetName -> targetActor
      if (targets.size <= 1) { // First registration. Makes sense to start sending the events.
        schedulePastDueEvents()
        timers.startSingleTimer(FetchTimer, FetchFirstBucket, persister.nextBucketReadWait)
      } else {
        pendingTargets.get(targetName) match {
          case None => // Do nothing
          case Some(pendingEvents) =>
            val currentTime = clock.currentTimeMillis
            pendingEvents.foreach { event =>
              require(event.target == targetName)
              deliverOrScheduleEvent(currentTime, event)
            }
            metrics.counter(pendingCount).dec(pendingEvents.size)
            pendingTargets -= targetName
        }
      }

    // Sent by timer to fetch the first regular bucket after startup.
    case FetchFirstBucket =>
      timers.startPeriodicTimer(FetchTimer, FetchBucket, bucketSize.millis)
      scheduleNextBucket()

    // Sent by timer to fetch the upcoming buckets.
    case FetchBucket =>
      scheduleNextBucket()
      // At this time all targets should be registered. Log any pending targets.
      pendingTargets foreach { case (target, buffer) =>
        log.warning("{} events pending for non-registered target {}", buffer.size, target)
      }

    // Sent by self to report the fetched buckets have arrived.
    case BucketEvents(bucketId, events) =>
      liveBuckets.get(bucketId) match {
        case Some(set) => set ++= events
        case None if events.nonEmpty => liveBuckets += bucketId -> events.to[mutable.Set]
        case _ =>
      }
      metrics.meter(retrieved).mark(events.size)
      metrics.histogram(bucketEventCount).update(events.size)
      val currentTime = clock.currentTimeMillis
      events.foreach(deliverOrScheduleEvent(currentTime, _))

      // Note: Event sequences:
      // RemoveStoredBucket ~> RemoveBucketFromIndex
      // RemoveStoredBucket ~> RemoveLiveBuckets

    // Sent by self to remove a stored bucket
    case RemoveStoredBucket(bucketId, live) =>
      persister.removeBucket(bucketId).map(_ =>
        if (live) RemoveLiveBuckets(Seq(bucketId)) else RemoveBucketFromIndex(bucketId)
      ) pipeTo self

    // Sent by self to remove a set of buckets from the liveBuckets and then from the index
    case RemoveLiveBuckets(buckets) =>
      liveBuckets --= buckets
      buckets.foreach(persister.removeBucketFromIndex)
      persister.persistIndex()

    // Sent by self to remove the bucket from the index
    case RemoveBucketFromIndex(bucketId) =>
      persister.removeBucketFromIndex(bucketId)
      persister.persistIndex()

    // Sent by the client to schedule an event
    case ScheduleEvent(msFromEpoch, target, eventKey) =>
      val event = ScheduledEvent(msFromEpoch, target, eventKey)
      val currentTime = clock.currentTimeMillis
      if (msFromEpoch <= currentTime + timeBuffer) {
        deliverOrScheduleEvent(currentTime, event)
      } else if (persister.bucket(msFromEpoch) <= lastScheduledBucket) {
        val bucketId = persister.bucket(event.msFromEpoch)
        liveBuckets.get(bucketId) match {
          case Some(set) => set += event
          case None => liveBuckets += bucketId -> mutable.Set(event)
        }
        persister.writeSchedule(event) pipeTo self
        deliverOrScheduleEvent(currentTime,event)
        metrics.meter(persisted).mark()
      } else {
        persister.writeSchedule(event) pipeTo self
        metrics.meter(persisted).mark()
      }
      metrics.meter(scheduled).mark()
      log.debug("Scheduled event {}", event)

    // Destination events sent by timer.
    case event: ScheduledEvent =>
      targets.get(event.target) match {
        case Some(actorRef) =>
          actorRef ! event
          removeEvent(event)
          metrics.meter(delivered).mark()
          log.debug("Delivered event {}", event)
        case None => addToPendingTargets(event)
      }

    // New bucket created.
    case Created(bucketId: Long) =>
      persister.addBucketToIndex(bucketId)

    // SBucket updated on write - no need to update index
    case _: Updated[_] =>

    case Status.Failure(e) =>
      log.error(e, "Persister Error: {}", e.toString)
  }

  def removeEvent(event: ScheduledEvent): Unit = {
    val bucketId = persister.bucket(event.msFromEpoch)
    liveBuckets.get(bucketId) match {
      case Some(eventSet) => // Most events to be removed should be in the live buckets.
        eventSet -= event
        if (eventSet.isEmpty) persister.removeBucket(bucketId).map(_ => RemoveLiveBuckets(Seq(bucketId))) pipeTo self
      case None => // But occasionally, we may get a non-live event. The cost is much higher.
        log.warning("High cost removal of individual event {}", event)
        val removeFuture = persister.removeIndividualEvent(bucketId, event)
        removeFuture.foreach { bucketEvents =>
          if (bucketEvents.isEmpty) self ! RemoveStoredBucket(bucketId, live = false)
        }
        removeFuture.failed.foreach { t => log.warning("Failed removing individual event: {}", t) }
    }
  }

  def schedulePastDueEvents(): Unit = {
    val currentTime = clock.currentTimeMillis
    val earliestEventTime = if (maxPastDue != -1L && maxPastDue < currentTime) currentTime - maxPastDue else 0L
    val firstBucket = persister.bucket(earliestEventTime)
    persister.pastDueBuckets.foreach { bucketId =>
      if (bucketId >= firstBucket) {
        lastScheduledBucket = bucketId
        persister.readSchedule(bucketId).map { events =>
          val eventsToDeliver =
            if (earliestEventTime >= 0L) events.filter(_.msFromEpoch >= earliestEventTime) else events
          if (eventsToDeliver.nonEmpty) BucketEvents(bucketId, eventsToDeliver) else RemoveStoredBucket(bucketId)
        } pipeTo self
      } else {
        persister.removeBucket(bucketId).map(_ => RemoveBucketFromIndex(bucketId)) pipeTo self
      }
    }
  }

  def scheduleNextBucket(): Unit = {
    val (bucketId, eventsFuture) = persister.nextBucketEvents
    lastScheduledBucket = bucketId
    eventsFuture.foreach {
      case events if events.nonEmpty => self ! BucketEvents(bucketId, events)
      case _ =>
    }

    // Also take this opportunity to cleanup any old empty buckets.
    val currentBucket = bucketId - 1
    val removedBuckets = liveBuckets.collect {
      case (b, set) if b < currentBucket - 1 && set.isEmpty => persister.removeBucket(b).map(_ => b)
    }
    if (removedBuckets.nonEmpty) {
      Future.sequence(removedBuckets).map(RemoveLiveBuckets(_)) pipeTo self
      log.warning("Removed {} empty buckets as part of scheduled cleanup.", removedBuckets.size)
    }
  }

  def deliverOrScheduleEvent(currentTime: Long, event: ScheduledEvent): Unit = {
    if (event.msFromEpoch <= currentTime + timeBuffer) self ! event
    else timers.startSingleTimer(event, event, (event.msFromEpoch - currentTime).millis)
  }

  def addToPendingTargets(event: ScheduledEvent): Unit = {
    pendingTargets.get(event.target) match {
      case Some(pendingEvents) => pendingEvents += event
      case None => pendingTargets += event.target -> mutable.Buffer(event)
    }
    metrics.counter(pendingCount).inc()
  }
}
