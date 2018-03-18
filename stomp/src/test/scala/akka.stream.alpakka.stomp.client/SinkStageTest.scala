package akka.stream.alpakka.stomp.client

import akka.Done
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete}
import io.vertx.ext.stomp.Frame
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future
import io.vertx.core.buffer.{Buffer => VertxBuffer}

import scala.collection.mutable.ArrayBuffer

class SinkStageTest extends ClientTest {

  "A Stomp Client SinkStage" should {
    "deliver message to a stomp server" in {

      // creating a stomp server
      import Server._
      var receivedFrameOnServer: ArrayBuffer[Frame] = ArrayBuffer()
      val port = 61666
      val server = getStompServer(Some(accumulateHandler(f => receivedFrameOnServer += f)), port)

      // connection settings
      val topic = "AnyTopic"
      val size = 10
      val settings = ConnectorSettings(connectionProvider = DetailsConnectionProvider("localhost",port))

      // functionality to test
      val sinkToStomp: Sink[Frame, Future[Done]] = Sink.fromGraph(new SinkStage(settings))
      val queueSource: Source[Frame, SourceQueueWithComplete[Frame]] =
        Source.queue[Frame](100, OverflowStrategy.backpressure)
      val (queue, sinkDone) = queueSource.toMat(sinkToStomp)(Keep.both).run()

      (1 to size)
        .map(i => s"$i")
        .map(VertxBuffer.buffer)
        .map({ vBufferStr =>
          new Frame().setDestination(topic).setCommand(Frame.Command.SEND).setBody(vBufferStr)
        })
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
  }

}
