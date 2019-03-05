package com.paypal.squbs.scheduler

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink}
import akka.util.ByteString

object EntityAnalyzer {

  // Usage: entity.transformDataBytes(EntityAnalyzer.capture)

  val captureLimit = 1024

  val captureSink: Sink[ByteString, NotUsed] = Flow[ByteString]
    .statefulMapConcat { () =>
      var capturedContent = ByteString.empty
      var emitted = false
      val mapFn = { bytes: ByteString =>
        if (capturedContent.size < captureLimit) {
          capturedContent ++= bytes
          List.empty[ByteString]
        } else if (!emitted) {
          emitted = true
          List(capturedContent)
        } else {
          List.empty[ByteString]
        }
      }
      mapFn
    }
    .map { firstFewBytes => logStats(firstFewBytes) }
    .to(Sink.ignore)

  def logStats(firstPart: ByteString): ByteString = {

    // Analyze the payload
    firstPart
  }

  val capture: Flow[ByteString, ByteString, NotUsed] = Flow[ByteString].alsoTo(captureSink)
}
