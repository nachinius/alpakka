package akka.stream.alpakka.stomp.client

import java.util.Optional

import akka.Done
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}
import akka.util.ByteString
import io.vertx.ext.stomp.{StompClient, StompClientConnection, StompClientOptions}

import scala.util.control.NonFatal

//import com.rabbitmq.client.AMQP.BasicProperties
//import com.rabbitmq.client._

import scala.compat.java8.OptionConverters
import scala.concurrent.{Future, Promise}

private[client] trait ConnectorLogic { this: GraphStageLogic =>
  private[client] var connection: StompClientConnection = _
//  private var client: StompClient = _

  def settings: ConnectorSettings
  def whenConnected: Unit
  def onFailure(ex: Throwable): Unit

  final override def preStart(): Unit =
    try {
      val client = settings.connectionProvider.get.connect({ ar =>
        if(ar.succeeded()) {
          connection = ar.result()
          whenConnected
        } else {
          throw ar.cause()
        }
      })
      // TCP level errors
      client exceptionHandler(ex => failStage(ex))

      // By protocol an error frame closes the connection
      client errorFrameHandler { frame => failStage(
        new Exception("ERROR FRAME " + frame.toString)
      ) }

    } catch {
      case NonFatal(e) =>
        onFailure(e)
        throw e
    }

  /** remember to call if overriding! */
  override def postStop(): Unit = {
    connection.disconnect()
  }
}

