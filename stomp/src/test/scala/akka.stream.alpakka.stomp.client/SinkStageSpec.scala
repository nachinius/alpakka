/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import akka.Done
import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete}
import io.vertx.core.Vertx
import io.vertx.core.buffer.{Buffer => VertxBuffer}
import io.vertx.ext.stomp._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration._

class SinkStageSpec extends WordSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with ScalaFutures {

  val system = ActorSystem(this.getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()(system)

  override implicit val patienceConfig = PatienceConfig(2.seconds)
  private implicit val executionContext = ExecutionContexts.sameThreadExecutionContext

  val vertx: Vertx = Vertx.vertx()
  // a default stomp server
  var server: StompServer = _

  var receivedFrameOnServer: ArrayBuffer[Frame] = ArrayBuffer()

  override def beforeEach(): Unit = {
    receivedFrameOnServer = ArrayBuffer()
    super.beforeEach()
  }
  override protected def beforeAll(): Unit = {
    val stompServerPromise = Promise[StompServer]()
    val stompServerFuture = stompServerPromise.future
    StompServer
      .create(vertx)
      .handler(
        StompServerHandler
          .create(vertx)
          .receivedFrameHandler(ar => {
            // accumulate SEND received by Server
//            println("SERVER ==> received frame " + ar.frame().toString)
            if (ar.frame().getCommand() == Frame.Command.SEND) {
              receivedFrameOnServer += ar.frame()
            }
          })
      )
      .listen(
        ar =>
          if (ar.succeeded()) {
            stompServerPromise success ar.result()
            server = ar.result()
          } else {
            stompServerPromise failure ar.cause()
        }
      )
    super.beforeAll()
    Await.ready(stompServerFuture, 2.seconds)
  }

  override protected def afterAll(): Unit = {
    system.terminate()
    server.close()
    vertx.close()
  }

  "A Stomp Client SinkStage" should {
    "deliver messages to a stomp server" in {

      val topic = "AnyTopic"
      val size = 10
      val settings = ConnectorSettings()

      import scala.concurrent.duration._
      val stompClientConnection: StompClientConnection = settings.connectionProvider.get(FiniteDuration(3, SECONDS))
      val sinkToStomp: Sink[Frame, Future[Done]] = Sink.fromGraph(new SinkStage(settings, stompClientConnection))
      val queueSource: Source[Frame, SourceQueueWithComplete[Frame]] =
        Source.queue[Frame](100, OverflowStrategy.backpressure)
      val queue: SourceQueueWithComplete[Frame] = queueSource.to(sinkToStomp).run()(materializer)

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

      queue.watchCompletion().futureValue shouldBe Done

      receivedFrameOnServer
        .result()
        .map(_.getBodyAsString)
        .map(_.toInt) should contain theSameElementsInOrderAs (
        1 to size
      )

    }
  }
}
