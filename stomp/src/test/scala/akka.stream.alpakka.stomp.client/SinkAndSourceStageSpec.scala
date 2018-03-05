/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source, SourceQueueWithComplete}
import io.vertx.core.{Vertx, VertxOptions}
import io.vertx.core.buffer.{Buffer => VertxBuffer}
import io.vertx.ext.stomp._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration._

class SinkAndSourceStageSpec extends WordSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with ScalaFutures {

  val system = ActorSystem(this.getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()(system)

  override implicit val patienceConfig = PatienceConfig(5.seconds)
  private implicit val executionContext = ExecutionContexts.sameThreadExecutionContext

  val vertx: Vertx = Vertx.vertx(new VertxOptions().setBlockedThreadCheckInterval(10000))
  // a default stomp server
  var receivedFrameOnServer: ArrayBuffer[Frame] = ArrayBuffer()

  override def beforeEach(): Unit = {
    receivedFrameOnServer = ArrayBuffer()
    super.beforeEach()
  }

  def accumulateHandler =
    StompServerHandler
      .create(vertx)
      .receivedFrameHandler(ar => {
        // accumulate SEND received by Server
        if (ar.frame().getCommand() == Frame.Command.SEND) {
          receivedFrameOnServer += ar.frame()
        }
      })

  def getStompServer(handler: Option[StompServerHandler] = None): StompServer =
    Await.result(stompServerFuture(handler), 2.seconds)

  private def stompServerFuture(handler: Option[StompServerHandler] = None) = {
    val serverHandler = handler.getOrElse(StompServerHandler.create(vertx))
    val promise = Promise[StompServer]()
    StompServer
      .create(vertx)
      .handler(serverHandler)
      .listen(
        ar =>
          if (ar.succeeded()) {
            promise success ar.result()
          } else {
            promise failure ar.cause()
        }
      )
    promise.future
  }

  override protected def beforeAll(): Unit = {}

  override protected def afterAll(): Unit = {
    system.terminate()
    vertx.close()
  }

  private def closeAwaitStompServer(server: StompServer) = {
    val promise = Promise[Done]()
    server.close(
      ar =>
        if (ar.succeeded()) promise.success(Done)
        else promise.failure(ar.cause())
    )
    Await.ready(promise.future, 2.seconds)
  }

  "A Stomp Client SinkStage" should {
    "deliver message to a stomp server" in {

      val server = getStompServer(Some(accumulateHandler))

      val topic = "AnyTopic"
      val size = 10
      val settings = ConnectorSettings()

      import scala.concurrent.duration._
      val sinkToStomp: Sink[Frame, Future[Done]] = Sink.fromGraph(new SinkStage(settings))
      val queueSource: Source[Frame, SourceQueueWithComplete[Frame]] =
        Source.queue[Frame](100, OverflowStrategy.backpressure)
      //      val queue: SourceQueueWithComplete[Frame] = queueSource.to(sinkToStomp).run()(materializer)
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

  "A Stomp Client SourceStage" should {
    "receive a message published at the stomp server in a topic" in {

      // create stomp server
      val server = getStompServer(
        Some(
          StompServerHandler
            .create(vertx)
            .destinationFactory((v, name) => {
              if (name.startsWith("/queue")) {
                Destination.queue(vertx, name)
              } else {
                Destination.topic(vertx, name)
              }
            })
        )
      )

      val topic = "topic2"
      val settings = ConnectorSettings().copy(topic = Some(topic))

      // testing source
      val sink = Sink.head[Frame]
      val source: Source[Frame, Future[Done]] = Source.fromGraph(new SourceStage(settings, 1))

      val (futConnected, futHead) = source.toMat(sink)(Keep.both).run()

      // to make a predictable test, wait until graph connects to stomp server
      Await.ready(futConnected, 2.seconds)

      // create another connection, to send to the stomp topic we have registered in SourceStage
      val futstompClient = settings.connectionProvider.getFuture
      val msg = "Hola source"
      val stomp = Await.result(futstompClient, 2.seconds)

      stomp.send(new Frame().setCommand(Frame.Command.SEND).setDestination(topic).setBody(VertxBuffer.buffer(msg)))

      futHead.futureValue.getBodyAsString shouldBe msg

      closeAwaitStompServer(server)
    }
  }
}
