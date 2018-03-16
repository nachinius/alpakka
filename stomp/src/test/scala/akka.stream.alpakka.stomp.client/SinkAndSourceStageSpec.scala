/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import java.util.UUID
import java.{lang, util}

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source, SourceQueueWithComplete}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import io.vertx.core.{AsyncResult, Handler, Vertx, VertxOptions}
import io.vertx.core.buffer.{Buffer => VertxBuffer}
import io.vertx.ext.auth.{AuthProvider, User}
import io.vertx.ext.stomp._
import io.vertx.ext.stomp.impl.Topic
import io.vertx.ext.stomp.impl.Topic.Subscription
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._

import scala.collection.immutable
import scala.collection.immutable.Queue
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration._

class SinkAndSourceStageSpec
    extends WordSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with ScalaFutures {

  implicit val system = ActorSystem(this.getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()(system)

  override implicit val patienceConfig = PatienceConfig(1.seconds)
  private implicit val executionContext = ExecutionContexts.sameThreadExecutionContext

  val vertx: Vertx = Vertx.vertx(new VertxOptions().setBlockedThreadCheckInterval(10))
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

  def getStompServer(handler: Option[StompServerHandler] = None, port: Int = 61613): StompServer =
    Await.result(stompServerFuture(handler,port), 2.seconds)

  private def stompServerFuture(handler: Option[StompServerHandler] = None, port: Int = 61613) = {
    val serverHandler = handler.getOrElse(StompServerHandler.create(vertx))
    val promise = Promise[StompServer]()
    StompServer
      .create(vertx, new StompServerOptions().setPort(port))
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

      val port = 61666
      val server = getStompServer(Some(accumulateHandler), port)

      val topic = "AnyTopic"
      val size = 10
      val settings = ConnectorSettings(connectionProvider = DetailsConnectionProvider("localhost",port))

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

  //========================================
  //========================================
  "A Stomp Client SourceStage" should {
    "receive a message published in a topic of a stomp server" in {

      val port = 61667
      // create stomp server
      val server = stompServerWithTopicAndQueue(port)

      val topic = "/topic/topic2"
      val settings = ConnectorSettings(
        DetailsConnectionProvider("localhost",port,None),
        Some(topic),
        false)


      // testing source
      val sink = Sink.head[Frame]
      val source: Source[Frame, Future[Done]] = Source.fromGraph(new SourceStage(settings ))

      val (futConnected: Future[Done], futHead: Future[Frame]) = source.toMat(sink)(Keep.both).run()

      // to make a predictable test, wait until graph connects to stomp server
      Await.ready(futConnected, 2.seconds)

      // create another connection, to send to the stomp topic we have registered in SourceStage
      val futstompClient = settings.connectionProvider.get
      val msg = "Hola source"
      val stomp = Await.result(futstompClient, 2.seconds)

      stomp.send(new Frame().setCommand(Frame.Command.SEND).setDestination(topic).setBody(VertxBuffer.buffer(msg)))

      futHead.futureValue.getBodyAsString shouldBe msg

      closeAwaitStompServer(server)
    }
    //========================================
    //========================================
    //========================================
    "receive sequentially the messages received in a stomp server when ack them" in {

      val port = 61613
      val server = stompServerWithTopicAndQueue(port).writingFrameHandler( ar => {
        println("----out")
        println(ar.frame().toString)
        println("-----finished out")
      })
      val topic = "/topic/mytopic"
      val connectionProvider = DetailsConnectionProvider("localhost",port)
      val settings = ConnectorSettings(
        connectionProvider = connectionProvider,
        withAck = true,
        topic = Some(topic))


      // testing source
      val sink = TestSink.probe[String]
      val source: Source[String, Future[Done]] = Source.fromGraph(new SourceStage(settings)).map(_.getBodyAsString)

      val (futConnected,sub) = source.toMat(sink)(Keep.both).run()

      // to make a predictable test, wait until graph connects to stomp server
      Await.ready(futConnected, 2.seconds)

      // create another connection, to send to the stomp topic we have registered in SourceStage
      val futstompClient = settings.connectionProvider.get
      val stomp = Await.result(futstompClient, 2.seconds)

      def sendToStomp(msg: String) = {
        stomp.send(new Frame().setCommand(Frame.Command.SEND).setDestination(topic).setBody(VertxBuffer.buffer(msg)))
      }

      sub.request(1)
      sendToStomp("1")
      sub.expectNext("1")
      sendToStomp("2")
      sub.requestNext("2")
      sendToStomp("3")
      sub.requestNext("3")

      sub.request(1)
      sendToStomp("4")
      sub.expectNext("4")
      sendToStomp("5")
      sendToStomp("6")
      sub.request(4)
      sendToStomp("7")
      sendToStomp("8")
      sendToStomp("9")
      sendToStomp("10")
      sendToStomp("11")
      sub.expectNext("5","6","7","8")
      sub.expectNoMessage(2.seconds)
      sub.requestNext("9")
      sub.requestNext("10")
      sub.requestNext("11")
      sub.request(1)
      sub.expectNoMessage(1.second)
      sendToStomp("12")
      sub.expectNext("12")

    }
  }

  private def stompServerWithTopicAndQueue(port: Int): StompServer = {
    getStompServer(
      Some(
        StompServerHandler
          .create(vertx)
          .destinationFactory((v, name) => {
            if (name.startsWith("/queue")) {
              Destination.queue(vertx, name)
            } else {
              new MyTopic(vertx, name)
            }
          })
      ), port
    )
  }
}

class MyTopic(vertx: Vertx, destination: String) extends Topic(vertx,destination) {
  import collection.mutable.{Queue => mQueue}
  case class Subs(stompServerConnection: StompServerConnection,
                  var waitingAck: Option[String] = None,
                  var pendingFrames: mQueue[Frame] = mQueue())

  var internalSubs: Map[Subscription, Subs] = Map()

  override def subscribe(connection: StompServerConnection, frame: Frame): Destination ={
    super.subscribe(connection, frame)
    // hack to be able to access the connection
    val i = subscriptions.size()
    val last = subscriptions.get(i-1)
    internalSubs = internalSubs + (last -> Subs(connection))
    this
  }

  override def ack(connection: StompServerConnection, frame: Frame): Boolean = {
    println("ack called for " + frame.getId)
    println(frame)
    val ack = frame.getId

    internalSubs.find(_._2.stompServerConnection == connection).find(_._2.waitingAck.contains(ack)).map {
      case (subscription: Subscription, subs: Subs) =>
        subs.waitingAck = None
        if(subs.pendingFrames.nonEmpty) deliver(subs,subscription,subs.pendingFrames.dequeue())
    }.nonEmpty
  }

  def deliver(subs: Subs, subscription: Subscription, frame: Frame) = {
    import collection.JavaConverters._

    val messageId = UUID.randomUUID().toString
    val message = Topic.transform(frame, subscription, messageId)
    if(message.getHeaders.asScala.contains(Frame.ACK)) {
      subs.waitingAck = Some(message.getAck)
    }
    subs.stompServerConnection.write(message)
  }
  def enqueue(subs: Subs, frame: Frame) = {
    subs.pendingFrames.enqueue(frame)
  }

  override def dispatch(connection: StompServerConnection, frame: Frame): Destination = {
    import collection.JavaConverters._
    val coll = for {
      subscription <- subscriptions.asScala
      sub: Subs <- internalSubs.get(subscription)
    } yield (sub,subscription)
    coll.foreach {
      case (subs: Subs, subscription: Subscription) if subs.waitingAck.isEmpty =>
        deliver(subs,subscription,frame)
      case (subs, _) => enqueue(subs, frame)
    }
    this
  }
}
