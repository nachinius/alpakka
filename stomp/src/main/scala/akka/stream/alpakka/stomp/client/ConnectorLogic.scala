/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import java.util.Optional

import akka.Done
import akka.stream.stage.GraphStageLogic
import io.vertx.ext.stomp.{Frame, StompClient, StompClientConnection}

import scala.util.control.NonFatal

private[client] trait ConnectorLogic {
  this: GraphStageLogic =>
  private[client] var connection: StompClientConnection = _

  def settings: ConnectorSettings

  def whenConnected: Unit

  def onFailure(ex: Throwable): Unit

  final override def preStart(): Unit =
    try {
      val client: StompClient = settings.connectionProvider.get.connect({ ar =>
        if (ar.succeeded()) {
          connection = ar.result()
          whenConnected
        } else {
          throw ar.cause()
        }
      })
      // TCP level errors
      client exceptionHandler (ex => failStage(ex))

      // By protocol an error frame closes the connection
      client errorFrameHandler { frame =>
        failStage(
          new Exception("ERROR FRAME " + frame.toString)
        )
      }

      //      client receivedFrameHandler( identity _ )
      //      client writingFrameHandler( identity _ )

    } catch {
      case NonFatal(e) =>
        onFailure(e)
        throw e
    }

  /** remember to call if overriding! */
  override def postStop(): Unit =
    connection.disconnect()

}

