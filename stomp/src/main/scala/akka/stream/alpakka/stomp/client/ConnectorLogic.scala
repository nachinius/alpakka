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

  val closeCallback = getAsyncCallback[StompClientConnection](_ => {
    promise.trySuccess(Done)
    completeStage()
  })
  val errorCallback = getAsyncCallback[Frame](frame => {
    acknowledge(frame)
    val ex = StompProtocolError(frame)
    failCallback.invoke(ex)
  })
  val dropCallback = getAsyncCallback[StompClientConnection](dropped => {
    val ex = StompClientConnectionDropped(dropped.toString)
    failCallback.invoke(ex)
  })
  val failCallback = getAsyncCallback[Throwable](ex => {
    promise.tryFailure(ex)
    failStage(ex)
  })
  val fullFillOnConnection = false
  var connection: StompClientConnection = _

  def settings: ConnectorSettings

  def promise: Promise[Done]

  def whenConnected(): Unit

  def onFailure(ex: Throwable): Unit

  override def preStart(): Unit = {

    val connectCallback = getAsyncCallback[StompClientConnection](conn => {
      connection = conn
      addHandlers
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

  def acknowledge(frame: Frame) = {
    if(settings.withAck && frame.getHeaders.containsKey(Frame.ACK)) {
      connection.ack(frame.getAck)
    }
  }

  def addHandlers() = {
    failHandler(connection)
    closeHandler(connection)
    errorHandler(connection)
    writeHandler(connection)
    dropHandler(connection)
    receiveHandler(connection)
    writeHandler(connection)
  }

  def writeHandler(connection: StompClientConnection) = ()

  def receiveHandler(connection: StompClientConnection)

  def dropHandler(connection: StompClientConnection) =
    connection.connectionDroppedHandler(dropped => dropCallback.invoke(dropped))

  def errorHandler(connection: StompClientConnection) =
    connection.errorHandler(frame => errorCallback.invoke(frame))

  def closeHandler(connection: StompClientConnection) =
    connection.closeHandler(conn => closeCallback.invoke(conn))

  private def failHandler(connection: StompClientConnection) =
    connection.exceptionHandler(ex => failCallback.invoke(ex))

  /** remember to call if overriding! */
  override def postStop(): Unit =
    if (connection.isConnected) connection.disconnect()

  /** remember to call if overriding! */
  override def postStop(): Unit =
    if (connection.isConnected) connection.disconnect()

  private def prepareExpectationOnReceipt(originalFrame: Frame) =
    if (originalFrame.getHeaders.containsKey(Frame.RECEIPT)) {
      expectedReceiptId = Some(originalFrame.getHeader(Frame.RECEIPT))
    }
}
