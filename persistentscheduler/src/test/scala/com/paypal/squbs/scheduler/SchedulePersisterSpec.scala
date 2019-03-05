package com.paypal.squbs.scheduler

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}
import scheduledevents.ScheduledEvent

class SchedulePersisterSpec extends TestKit(ActorSystem("SchedulePersisterSpec"))
  with FlatSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with ScalaFutures {

  import system.dispatcher
  import MapBasedRCMW.dataStore

  val bucketSize = 1000 // 1 second for quick testing.

  val clock = new TestClock
  val nodeId = new TestNodeIdProvider
  val persister = new SchedulePersister((new MapBasedRCMWFactory).create, nodeId.clusterName, nodeId.nodeId,
                                        bucketSize, clock)

  import persister._

  private val currentTime = clock.currentTimeMillis

  override def beforeEach(): Unit = dataStore.clear()

  override def afterAll(): Unit = system.terminate()

  it should "write both bucket set and scheduled data when scheduling" in {
    val eventTime = currentTime + 2 * bucketSize
    val event = ScheduledEvent(eventTime, "someTarget", "someKey")
    val writeResult = persister.writeSchedule(event)
    writeResult.futureValue shouldBe Done

    dataStore should have size 2
    dataStore should contain key bucketSetKey
    dataStore should contain key bucketKey(bucket(eventTime))
  }

  it should "update bucket set and add different slot of scheduled data" in {
    val preEvent = ScheduledEvent(currentTime + 2 * bucketSize, "someTarget", "someKey")
    persister.writeSchedule(preEvent).futureValue shouldBe Done

    val prevBucketSet = dataStore.get(bucketSetKey)
    val eventTime = currentTime + 3 * bucketSize
    val event = ScheduledEvent(eventTime, "someTarget", "someKey2")
    val writeResult = persister.writeSchedule(event)
    writeResult.futureValue shouldBe Done

    dataStore should have size 3
    dataStore.get(bucketSetKey).size should be > prevBucketSet.size
    dataStore should contain key bucketKey(bucket(eventTime))
  }

  it should "remove bucket from store and bucket set on removeBucket" in {
    val preEvent = ScheduledEvent(currentTime + 2 * bucketSize, "someTarget", "someKey")
    persister.writeSchedule(preEvent).futureValue shouldBe Done
    val preEvent2 = ScheduledEvent(currentTime + 3 * bucketSize, "someTarget", "someKey2")
    persister.writeSchedule(preEvent2).futureValue shouldBe Done
    val prevBucketSet = dataStore.get(bucketSetKey)

    val eventTime = currentTime + 2 * bucketSize
    val bucketId = bucket(eventTime)
    val writeResult = removeBucket(bucketId)
    writeResult.futureValue shouldBe Done

    dataStore should have size 2
    dataStore shouldNot contain key bucketKey(bucketId)
    dataStore.get(bucketSetKey).size should be < prevBucketSet.size
  }

  it should "not remove bucket given bucket is not empty after remove individual event" in {
    val preEvent = ScheduledEvent(currentTime + 2 * bucketSize, "someTarget", "someKey")
    persister.writeSchedule(preEvent).futureValue shouldBe Done
    val preEvent2 = ScheduledEvent(currentTime + 3 * bucketSize, "someTarget", "someKey2")
    persister.writeSchedule(preEvent2).futureValue shouldBe Done

    val eventTime = currentTime + 3 * bucketSize
    val event = ScheduledEvent(eventTime, "someTarget", "someKey3")
    val bucketId = bucket(eventTime)
    val writeResult = persister.writeSchedule(event)
    writeResult.futureValue shouldBe Done

    // Capture the doc sizes before removal
    val eventsDocSize = dataStore.get(bucketKey(bucketId)).size
    val bucketSetDocSize = dataStore.get(bucketSetKey).size

    val removeResult = removeIndividualEvent(bucketId, event)

    // Check doc sizes after removal
    removeResult.futureValue shouldBe Done
    dataStore.get(bucketKey(bucketId)).size should be < eventsDocSize
    dataStore.get(bucketSetKey).size shouldBe bucketSetDocSize
  }

  it should "remove bucket given bucket is empty after remove individual event" in {
    val preEvent = ScheduledEvent(currentTime + 2 * bucketSize, "someTarget", "someKey")
    persister.writeSchedule(preEvent).futureValue shouldBe Done

    val eventTime = currentTime + 3 * bucketSize
    val bucketId = bucket(eventTime)
    val event = ScheduledEvent(eventTime, "someTarget", "someKey2")
    persister.writeSchedule(event).futureValue shouldBe Done


    // Capture the doc sizes before removal
    val bucketSetDocSize = dataStore.get(bucketSetKey).size

    val removeResult = removeIndividualEvent(bucketId, event)
    removeResult.futureValue shouldBe Done

    dataStore shouldNot contain key bucketKey(bucketId)
    dataStore.get(bucketSetKey).size should be < bucketSetDocSize
  }

  it should "read the stored events back correctly" in {
    val eventTime = currentTime + 2 * bucketSize
    val bucketId = bucket(eventTime)

    val events = 1 to 10 map { i => ScheduledEvent(eventTime + i, "someTarget", s"someKeys$i") }
    events.foreach { persister.writeSchedule(_).futureValue shouldBe Done }

    readSchedule(bucketId).futureValue should contain theSameElementsAs events
  }

  it should "read the next bucket events correctly" in {
    val eventTime = currentTime + bucketSize
    val currentBucketId = bucket(currentTime)
    val events = 1 to 10 map { i => ScheduledEvent(eventTime + i, "someTarget", s"someKeys$i") }
    events.foreach { persister.writeSchedule(_).futureValue shouldBe Done }

    val (bucketId, eventsF) = nextBucketEvents

    bucketId shouldBe currentBucketId + 1
    eventsF.futureValue should contain theSameElementsAs events
  }

  it should "read the past-due buckets correctly" in {
    val buckets = 1 to 5 map { i =>
      bucket(currentTime) - (10 - i)
    }
    val events = 1 to 5 map { b =>
      val eventTime = currentTime - (10 - b) * bucketSize
      ScheduledEvent(eventTime, "someTarget", s"someKeys$b")
    }
    events.foreach { persister.writeSchedule(_).futureValue shouldBe Done }

    pastDueBuckets should contain theSameElementsInOrderAs buckets
  }
}
