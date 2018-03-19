/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import akka.Done
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete}
import io.vertx.ext.stomp.Frame
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.{Future, Promise}
import io.vertx.core.buffer.{Buffer => VertxBuffer}

import scala.collection.mutable.ArrayBuffer

class SinkStageSpec extends StompClientSpec {

  "A Stomp Client SinkStage" should {
    "deliver messages to a stomp server and complete when downstream completes" in {

      // creating a stomp server
      import Server._
      var receivedFrameOnServer: ArrayBuffer[Frame] = ArrayBuffer()
      val port = 61666
      val server = getStompServer(Some(accumulateHandler(f => receivedFrameOnServer += f)), port)

      // connection settings
      val topic = "AnyTopic"
      val size = 10
      val settings = ConnectorSettings(connectionProvider = DetailsConnectionProvider("localhost", port))

      // functionality to test
      val sinkToStomp: Sink[SendingFrame, Future[Done]] = Sink.fromGraph(new SinkStage(settings))
      val queueSource: Source[SendingFrame, SourceQueueWithComplete[SendingFrame]] =
        Source.queue[SendingFrame](100, OverflowStrategy.backpressure)
      val (queue, sinkDone) = queueSource.toMat(sinkToStomp)(Keep.both).run()

      (1 to size)
        .map(i => s"$i")
        .map(VertxBuffer.buffer)
        .map({ vBufferStr =>
          new Frame().setDestination(topic).setCommand(Frame.Command.SEND).setBody(vBufferStr)
        })
        .map { SendingFrame.from }
        .map { frame =>
          queue.offer(frame)
        }
      queue.complete()
//      queue.watchCompletion().futureValue shouldBe Done
      sinkDone.futureValue shouldBe Done

      receivedFrameOnServer
        .result()
        .map(_.getBodyAsString)
        .map(_.toInt) should contain theSameElementsInOrderAs (1 to size)

      closeAwaitStompServer(server)
    }

    "fail when no destination is set" in {
      // creating a stomp server
      import Server._
      val port = 61667
      val server = getStompServer(None, port)

      val settings = ConnectorSettings(connectionProvider = DetailsConnectionProvider("localhost", port))

      // functionality to test
      val sinkToStomp = Sink.fromGraph(new SinkStage(settings))
      val queueSource = Source.queue[SendingFrame](100, OverflowStrategy.backpressure)
      val (queue, sinkDone) = queueSource.toMat(sinkToStomp)(Keep.both).run()

      queue.offer(SendingFrame(Map(),"msg".toCharArray.toVector.map(_.toByte)))

      sinkDone.failed.futureValue shouldBe an[StompProtocolError]

      closeAwaitStompServer(server)
    }

    "settings topic should set frame destination if not already present" in {
      // creating a stomp server
      import Server._
      var receivedFrameOnServer: ArrayBuffer[Frame] = ArrayBuffer()
      val port = 61667
      val server = getStompServer(Some(accumulateHandler(f => receivedFrameOnServer += f)), port)

      // connection settings
      val topic = "AnyTopic"
      val settings = ConnectorSettings(connectionProvider = DetailsConnectionProvider("localhost", port),
        topic = Some(topic))

      // functionality to test
      val sinkToStomp = Sink.fromGraph(new SinkStage(settings))
      val queueSource = Source.queue[SendingFrame](100, OverflowStrategy.backpressure)
      val (queue, sinkDone) = queueSource.toMat(sinkToStomp)(Keep.both).run()

      queue.offer(SendingFrame(Map(("destination" -> "another destination")),"msg".toCharArray.toVector.map(_.toByte)))
      queue.offer(SendingFrame(Map(),"msg".toCharArray.toVector.map(_.toByte)))
      queue.complete()
      queue.watchCompletion().futureValue shouldBe Done
      sinkDone.futureValue shouldBe Done

      receivedFrameOnServer.result().map(_.getDestination) should contain theSameElementsInOrderAs Seq("another destination","AnyTopic")


      closeAwaitStompServer(server)
    }

  }

}
