/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import akka.Done
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}
import io.vertx.ext.stomp.Frame

import scala.concurrent.{Future, Promise}

// @TODO: must change vertx.*.Frame to a immutable one at the stream level
final class SinkStage(settings: ConnectorSettings)
    extends GraphStageWithMaterializedValue[SinkShape[Frame], Future[Done]] {
  stage =>

  override def shape: SinkShape[Frame] = SinkShape.of(in)

  override def toString: String = "StompClientSink"

  override protected def initialAttributes: Attributes = SinkStage.defaultAttributes

  val in = Inlet[Frame]("StompClientSink.in")

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val thePromise = Promise[Done]()
    (new GraphStageLogic(shape) with ConnectorLogic {
      override val settings: ConnectorSettings = stage.settings
      override val acceptedCommands: Set[Frame.Command] = Set(Frame.Command.SEND)
      override val promise = thePromise
      //      private val destination = settings.destination
      //      private val requestReceiptHandler: Option[Frame => ()] = settings.requestReceiptHandler

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
            val originalFrame = grab(in)
            //            checkCommand(originalFrame)
            //            prepareExpectationOnReceipt(originalFrame)
            if (settings.topic.nonEmpty)
              originalFrame.setDestination(settings.topic.get)
            connection.send(originalFrame)
            if (expectedReceiptId.isEmpty) {
              pull(in)
            } else {
              // will call pull(in) when receiving receipt
              // it's handle at connection.receivedFrameHandler
            }
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

// @TODO: better semantics to this exception
sealed trait StompThrowable extends Throwable

case class StompProtocolError(frame: Frame) extends StompThrowable

case class StompClientConnectionDropped(str: String = "") extends StompThrowable

case class IncorrectCommand(str: String = "Only SEND command is accepted in StompSink") extends StompThrowable

case class StompBadReceipt(str: String) extends StompThrowable
