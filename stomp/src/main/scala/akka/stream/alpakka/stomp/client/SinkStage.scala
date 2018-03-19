/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import akka.Done
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}
import io.vertx.ext.stomp.{StompClientConnection}

import scala.concurrent.{Future, Promise}

final class SinkStage(settings: ConnectorSettings)
    extends GraphStageWithMaterializedValue[SinkShape[SendingFrame], Future[Done]] {
  stage =>

  override def shape: SinkShape[SendingFrame] = SinkShape.of(in)

  override def toString: String = "StompClientSink"

  override protected def initialAttributes: Attributes = SinkStage.defaultAttributes

  val in = Inlet[SendingFrame]("StompClientSink.in")

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val thePromise = Promise[Done]()
    (new GraphStageLogic(shape) with ConnectorLogic {
      override val settings: ConnectorSettings = stage.settings
      override val promise = thePromise
      //      private val destination = settings.destination
      //      private val requestReceiptHandler: Option[Frame => ()] = settings.requestReceiptHandler

      override def receiveHandler(connection: StompClientConnection): Unit = ()

      override def whenConnected: Unit = pull(in)

      setHandler(
        in,
        new InHandler {

          override def onUpstreamFailure(ex: Throwable): Unit = {
            promise.failure(ex)
            super.onUpstreamFailure(ex)
          }

          override def onUpstreamFinish(): Unit = {
            promise.success(Done)
            super.onUpstreamFinish()
          }

          override def onPush(): Unit = {
            val originalFrame: SendingFrame = grab(in)
            //            checkCommand(originalFrame)
            //            prepareExpectationOnReceipt(originalFrame)
            val vertxFrame = originalFrame.toVertexFrame
            if (settings.topic.nonEmpty) {
              vertxFrame.setDestination(settings.topic.get)
            }
            connection.send(vertxFrame)
            pull(in)
          }
        }
      )

      override def postStop(): Unit = {
        //        if(connection != null) connection.disconnect()
        promise.tryFailure(new RuntimeException("stage stopped unexpectedly"))
        super.postStop()
      }

      override def onFailure(ex: Throwable): Unit =
        promise.tryFailure(ex)

    }, thePromise.future)
  }

}

object SinkStage {
  private val defaultAttributes =
    Attributes.name("StompClientSink").and(ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher"))
}
