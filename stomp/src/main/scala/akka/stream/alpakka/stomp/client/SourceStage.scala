/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import akka.Done
import akka.stream._
import akka.stream.stage._
import io.vertx.ext.stomp.Frame

import scala.collection.{mutable, JavaConverters}
import scala.concurrent.{Future, Promise}

final class SourceStage(settings: ConnectorSettings, bufferSize: Int)
    extends GraphStageWithMaterializedValue[SourceShape[Frame], Future[Done]] {
  stage =>

  val out = Outlet[Frame]("StompClientSource.out")

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val thePromise = Promise[Done]()
    val graphStageLogic = new GraphStageLogic(shape) with ConnectorLogic {

      override val settings: ConnectorSettings = stage.settings

      override val promise = thePromise
      override val fullFillOnConnection: Boolean = true

      override def acceptedCommands: Set[Frame.Command] = Set(Frame.Command.MESSAGE)

      private val queue = mutable.Queue[Frame]()

      private val headers: mutable.Map[String, String] =
        if (settings.withAck) mutable.Map(Frame.ACK -> "client") else mutable.Map()

      override def whenConnected: Unit = {
        val receiveMessageCallback = getAsyncCallback[Frame] {
          if (settings.withAck) { frame =>
            {
              connection.ack(frame.getId)
              handleDelivery(frame)
            }
          } else
            frame => {
              handleDelivery(frame)
            }
        }

        import JavaConverters._
        connection.subscribe(
          settings.topic.get,
          headers.asJava, { frame =>
            receiveMessageCallback.invoke(frame)
          }
        )
        connection.receivedFrameHandler(frame => receiveMessageCallback.invoke(frame))
      }

      def handleDelivery(message: Frame): Unit =
        if (isAvailable(out)) {
          pushMessage(message)
        } else if (queue.size + 1 > bufferSize) {
          failStage(new RuntimeException(s"Reached maximum buffer size $bufferSize"))
        } else {
          queue.enqueue(message)
        }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = if (queue.nonEmpty) {
            pushMessage(queue.dequeue())
          }

          override def onDownstreamFinish(): Unit =
            completeStage()
        }
      )

      def pushMessage(frame: Frame): Unit =
        push(out, frame)

      override def postStop(): Unit =
        promise.trySuccess(Done)

      super.postStop()

      override def onFailure(ex: Throwable): Unit = {
        promise.trySuccess(Done)
        ()
      }

    }
    (graphStageLogic, thePromise.future)
  }

  override def shape: SourceShape[Frame] = SourceShape.of(out)

  override def toString: String = "StompClientSink"

  override protected def initialAttributes: Attributes = SourceStage.defaultAttributes

}

object SourceStage {
  private val defaultAttributes =
    Attributes.name("StompClientSource").and(ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher"))
}
