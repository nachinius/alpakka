/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import akka.Done
import akka.stream.stage.GraphStageLogic
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.stomp.{Frame, StompClient, StompClientConnection, StompClientOptions}

import scala.concurrent.Promise

private[client] trait ConnectorLogic {
  this: GraphStageLogic =>

  var expectedReceiptId: Option[String] = None
  var connection: StompClientConnection = _

  final override def preStart(): Unit = {
    def addHandlers(connection: StompClientConnection) = {
      val failCallback = getAsyncCallback[Throwable](ex => {
        promise.failure(ex)
        failStage(ex)
      })
      val closeCallback = getAsyncCallback[Unit](_ => {
        promise.success(Done)
        completeStage()
      })
      val errorCallback = getAsyncCallback[Frame](frame => {
        val ex = StompProtocolError(frame)
        failCallback.invoke(ex)
      })
      val dropCallback = getAsyncCallback[StompClientConnection](dropped => {
        val ex = StompClientConnectionDropped(dropped.toString)
        failCallback.invoke(ex)
      })
      val checkRequestIdCallback = getAsyncCallback[Frame](frame => {
        checkForRequestIdIfExpected(frame)
      })

      connection.exceptionHandler(ex => failCallback.invoke(ex))
      connection.closeHandler(_ => closeCallback.invoke(()))
      connection.errorHandler(frame => errorCallback.invoke(frame))

      // for implementing debugging functionality
      // connection.writingFrameHandler(frame => doSomethingWithFrameLikeChangingOrInspecting(frame))

      connection.connectionDroppedHandler(dropped => dropCallback.invoke(dropped))
      connection.receivedFrameHandler(frame => checkRequestIdCallback.invoke(frame))
    }

    val connectCallback = getAsyncCallback[StompClientConnection](conn => {
      connection = conn
      addHandlers(connection)
      whenConnected
    })

    // connecting async
    settings.connectionProvider.getStompClient
      .connect(
        ar => {
          if (ar.succeeded()) {
            connectCallback.invoke(ar.result())
          } else {
            throw ar.cause()
          }
        }
      )
  }

  /** remember to call if overriding! */
  override def postStop(): Unit =
    if (connection.isConnected) connection.disconnect()

  def checkCommand(frame: Frame) =
    if (!acceptedCommands.contains(frame.getCommand)) {
      failStage(IncorrectCommand())
    }

  private def prepareExpectationOnReceipt(originalFrame: Frame) =
    if (originalFrame.getHeaders.containsKey(Frame.RECEIPT)) {
      expectedReceiptId = Some(originalFrame.getHeader(Frame.RECEIPT))
    }

  def checkForRequestIdIfExpected(frame: Frame) =
    expectedReceiptId.map { expected =>
      if (frame.getCommand == Frame.Command.RECEIPT) {
        if (frame.getHeaders.containsKey(Frame.RECEIPT_ID) && expected == frame.getHeader(Frame.RECEIPT_ID)) {
          expectedReceiptId = None
          Right(())
        } else Left("Bad receipt id or missing header")
      } else Left("Not a receipt message")
    } foreach { sol: Either[String, Unit] =>
      sol match {
        case Right(_) => ()
        case Left(str) => failStage(StompBadReceipt(str))
      }
    }

  def settings: ConnectorSettings

  def acceptedCommands: Set[Frame.Command]

  def promise: Promise[Done]

  def whenConnected: Unit

  def onFailure(ex: Throwable): Unit

}
