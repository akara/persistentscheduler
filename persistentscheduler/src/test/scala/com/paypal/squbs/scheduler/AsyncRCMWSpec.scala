package com.paypal.squbs.scheduler

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures

class AsyncRCMWSpec extends TestKit(ActorSystem("AsyncRCMWSpec"))
  with FlatSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures {

  val rcmw = (new MapBasedRCMWFactory).create
  val rcmwKey = "key"
  val initData = "value1"
  val addData = "value2"
  val updatedData = initData + '\n' + addData

  it should "create a new document if key does not exist" in {
    val result = rcmw(
      rcmwKey,
      createData = _ => (ByteString(initData), initData),
      modData = { (_, currentBytes) =>
        val newData = currentBytes.utf8String + "\n" + addData
        (ByteString(newData), newData)
      }
    )
    result.futureValue shouldBe Created(initData)
  }

  it should "update the document if key exists" in {
    val result = rcmw(
      rcmwKey,
      createData = _ => (ByteString(initData), initData),
      modData = { (_, currentBytes) =>
        val newData = currentBytes.utf8String + "\n" + addData
        (ByteString(newData), newData)
      }
    )
    result.futureValue shouldBe Updated(updatedData)
  }

  it should "read the latest document if key exists" in {
    val result = rcmw.read(rcmwKey)
    result.futureValue shouldBe Some(ByteString(updatedData))
  }

  it should "provide a None on read if key does not exist" in {
    val result = rcmw.read("does_not_exist")
    result.futureValue shouldBe None
  }

  it should "successfully delete the document" in {
    val result = rcmw.delete(rcmwKey)
    result.futureValue shouldBe Done
  }

  it should "return an exception when deleting a non-existing document" in {
    val result = rcmw.delete("does_not_exist")
    result.failed.futureValue shouldBe a[NoSuchElementException]
  }

  override def afterAll(): Unit = system.terminate()
}
