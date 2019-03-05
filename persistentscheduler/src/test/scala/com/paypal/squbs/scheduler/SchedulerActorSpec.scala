package com.paypal.squbs.scheduler

import akka.Done
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}
import scheduledevents.ScheduledEvent

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object SchedulerActorSpec {

  val rcmwFactory = new MapBasedRCMWFactory
  val nodeId = new TestNodeIdProvider
  val clock = new WallClock
  val bucketSize = 1000
  val maxRecoverBuckets = 12


  val config: Config = ConfigFactory.parseString(
    s"""
      |persistent-scheduler {
      |  node-id-provider = ${nodeId.getClass.getName}
      |  rcmw-factory = ${rcmwFactory.getClass.getName}
      |  bucket-size = ${bucketSize}ms
      |  time-buffer = 5ms
      |  max-recovery = ${maxRecoverBuckets * bucketSize}ms
      |}
    """.stripMargin)
}

class SchedulerActorSpec extends TestKit(ActorSystem("SchedulerActorSpec", SchedulerActorSpec.config)) with FlatSpecLike
  with ImplicitSender with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with Eventually with ScalaFutures {

  import MapBasedRCMW.dataStore
  import SchedulerActorSpec._
  import system.dispatcher

  val testPersister = new SchedulePersister(rcmwFactory.create, nodeId.clusterName, nodeId.nodeId, bucketSize)

  override def beforeEach(): Unit = dataStore.clear()

  override def afterAll(): Unit = system.terminate()

  it should "fire past-due events" in {
    val currentTime = clock.currentTimeMillis

    // Prepare old events
    val events = 1 to 5 map { b =>
      val eventTime = currentTime - (10 - b) * bucketSize
      ScheduledEvent(eventTime, "thisTarget", s"someKeys$b")
    }
    events.foreach { testPersister.writeSchedule(_).futureValue shouldBe Done }

    // Test fetching of the old events
    val schedulerActor = system.actorOf(Props[SchedulerActor])
    schedulerActor ! RegisterForEvents("thisTarget", self)
    expectMsgAllOf(5.seconds, events: _*)

    schedulerActor ! PoisonPill
  }

  it should "fire past-due events to two targets upon registration" in {
    val currentTime = clock.currentTimeMillis

    // Prepare old events
    val events = 1 to 5 map { b =>
      val eventTime = currentTime - (10 - b) * bucketSize
      if (b % 2 == 0) ScheduledEvent(eventTime, "thisTarget", s"someKeys$b")
      else ScheduledEvent(eventTime, "secondTarget", s"someKeys$b")
    }
    events.foreach { testPersister.writeSchedule(_).futureValue shouldBe Done }

    val (thisTargetEvents, secondTargetEvents) = events.partition(_.target == "thisTarget")

    val schedulerActor = system.actorOf(Props[SchedulerActor])

    // Test fetching old events for "thisTarget"
    schedulerActor ! RegisterForEvents("thisTarget", self)
    expectMsgAllOf(5.seconds, thisTargetEvents: _*)

    // Test fetching old events for "secondTarget"
    schedulerActor ! RegisterForEvents("secondTarget", self)
    expectMsgAllOf(5.seconds, secondTargetEvents: _*)

    schedulerActor ! PoisonPill
  }

  it should "not fire expired past-due events and delete buckets correctly" in {
    val currentTime = clock.currentTimeMillis

    // Prepare old events
    val events = 1 to 10 flatMap { b =>
      val eventTime = currentTime - (15 - b) * bucketSize
      val eventTime2 = currentTime + 2 - (15 - b) * bucketSize
      Seq(
        ScheduledEvent(eventTime, "thisTarget", s"someKeys$b"),
        ScheduledEvent(eventTime2, "thisTarget", s"someKeys$b-2")
      )
    }
    events.foreach { testPersister.writeSchedule(_).futureValue shouldBe Done }

    // Test fetching of the old events
    val schedulerActor = system.actorOf(Props[SchedulerActor])
    schedulerActor ! RegisterForEvents("thisTarget", self)

    val received = receiveWhile(max = 2500.millis) {
      case e: ScheduledEvent => e
    }

    // Match the latest n events.
    implicit val ordering: Ordering[Long] = Ordering.Long.reverse
    val sortedReceived = received.sortBy(_.msFromEpoch)
    val relevantEvents = events.sortBy(_.msFromEpoch).take(sortedReceived.size)

    sortedReceived should contain theSameElementsInOrderAs relevantEvents

    schedulerActor ! PoisonPill
  }

  it should "not miss events during startup" in {
    val currentTime = clock.currentTimeMillis

    // Prepare old events
    val events = 1 to 20 map { b =>
      val eventTime = currentTime - (10 - b) * bucketSize
      ScheduledEvent(eventTime, "thisTarget", s"someKeys$b")
    }
    events.foreach { testPersister.writeSchedule(_).futureValue shouldBe Done }

    val schedulerActor = system.actorOf(Props[SchedulerActor])
    schedulerActor ! RegisterForEvents("thisTarget", self)
    expectMsgAllOf(15.seconds, events: _*)

    // Make sure the buckets are eventually removed
    eventually {
      dataStore.keySet.asScala.filterNot(_ == s"${nodeId.clusterName}_${nodeId.nodeId}_buckets") shouldBe empty
    }

    schedulerActor ! PoisonPill
  }

  it should "not miss events scheduled around current time" in {
    val currentTime = clock.currentTimeMillis

    // Prepare old events
    val events = 1 to 20 map { b =>
      val eventTime = currentTime - (10 - b) * bucketSize
      ScheduledEvent(eventTime, "thisTarget", s"someKeys$b")
    }
    events.foreach { testPersister.writeSchedule(_).futureValue shouldBe Done }

    val newEvents = 1 to 5 map { b =>
      val eventTime = currentTime - (1 - b) * bucketSize
      ScheduledEvent(eventTime, "thisTarget", s"someNewKeys$b")
    }

    val schedulerActor = system.actorOf(Props[SchedulerActor])
    schedulerActor ! RegisterForEvents("thisTarget", self)

    // Schedule new events
    newEvents.foreach { event => schedulerActor ! ScheduleEvent(event.msFromEpoch, event.target, event.eventKey)}

    // Expect saved and new events
    expectMsgAllOf(15.seconds, events ++ newEvents: _*)

    // Make sure the buckets are eventually removed
    eventually {
      dataStore.keySet.asScala.filterNot(_ == s"${nodeId.clusterName}_${nodeId.nodeId}_buckets") shouldBe empty
    }

    schedulerActor ! PoisonPill
  }
}
