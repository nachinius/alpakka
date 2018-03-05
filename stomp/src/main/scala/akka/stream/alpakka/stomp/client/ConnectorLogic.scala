/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import akka.Done
import akka.stream.stage.GraphStageLogic
import io.vertx.ext.stomp.{Frame, StompClientConnection}

import scala.concurrent.Promise

private[client] trait ConnectorLogic {
  this: GraphStageLogic =>

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
  val failCallback = getAsyncCallback[Throwable](ex => {
    promise.failure(ex)
    failStage(ex)
  })
  val fullFillOnConnection = false
  var expectedReceiptId: Option[String] = None
  var connection: StompClientConnection = _

  def settings: ConnectorSettings

  def acceptedCommands: Set[Frame.Command]

  def promise: Promise[Done]

  def whenConnected: Unit

  def onFailure(ex: Throwable): Unit

  override def preStart(): Unit = {

    val connectCallback = getAsyncCallback[StompClientConnection](conn => {
      connection = conn
      addHandlers(connection)
      whenConnected
      if (fullFillOnConnection) promise.trySuccess(Done)
    })

    // connecting async
    settings.connectionProvider.getStompClient
      .connect(
        ar => {
          if (ar.succeeded()) {
            connectCallback.invoke(ar.result())
          } else {
            if (fullFillOnConnection) promise.tryFailure(ar.cause)
            throw ar.cause()
          }
        }
      )
  }

  def addHandlers(connection: StompClientConnection) = {
    failHandler(connection)
    closeHandler(connection)
    errorHandler(connection)
    // for implementing debugging functionality
    // connection.writingFrameHandler(frame => doSomethingWithFrameLikeChangingOrInspecting(frame))
    dropHandler(connection)
    receiveHandler(connection)
    writeHandler(connection)
  }

  def writeHandler(connection: StompClientConnection) =
    ()

  def receiveHandler(connection: StompClientConnection) =
    connection.receivedFrameHandler(frame => checkRequestIdCallback.invoke(frame))

  def dropHandler(connection: StompClientConnection) =
    connection.connectionDroppedHandler(dropped => dropCallback.invoke(dropped))

  def errorHandler(connection: StompClientConnection) =
    connection.errorHandler(frame => errorCallback.invoke(frame))

  def closeHandler(connection: StompClientConnection) =
    connection.closeHandler(_ => closeCallback.invoke(()))

  private def failHandler(connection: StompClientConnection) =
    connection.exceptionHandler(ex => failCallback.invoke(ex))

  /** remember to call if overriding! */
  override def postStop(): Unit =
    if (connection != null && connection.isConnected) connection.disconnect()

  def checkCommand(frame: Frame) =
    if (!acceptedCommands.contains(frame.getCommand)) {
      failStage(IncorrectCommand())
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

  private def prepareExpectationOnReceipt(originalFrame: Frame) =
    if (originalFrame.getHeaders.containsKey(Frame.RECEIPT)) {
      expectedReceiptId = Some(originalFrame.getHeader(Frame.RECEIPT))
    }

}
