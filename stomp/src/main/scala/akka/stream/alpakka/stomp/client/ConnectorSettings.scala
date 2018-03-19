/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.stomp.{StompClient, StompClientConnection, StompClientOptions}

import scala.concurrent.{Await, Future, Promise}

object ConnectorSettings {

  def apply(): ConnectorSettings =
    ConnectorSettings(connectionProvider = LocalConnectionProvider)
}

case class ConnectorSettings(
    connectionProvider: ConnectionProvider,
    topic: Option[String] = None,
    withAck: Boolean = false
)

trait ConnectionProvider {
  def vertx: Vertx = Vertx.vertx()
  val noHeartBeatObject = new JsonObject().put("x", 0).put("y", 0)

  def stompClientOptions: StompClientOptions

  def get: Future[StompClientConnection] = {
    val promise = Promise[StompClientConnection]()
    getStompClient.connect(
      ar => {
        if (ar.succeeded()) {
          promise.trySuccess(ar.result())
        } else {
          promise.tryFailure(ar.cause())
        }
      }
    )
    promise.future
  }

  private[client] def getStompClient: StompClient = StompClient.create(vertx, stompClientOptions)

  def release(connection: StompClientConnection): StompClientConnection = connection.disconnect()

  def release(connection: StompClient): Unit = connection.close()
}

/**
 * Connects to a local STOMP server at the default port with no password.
 */
case object LocalConnectionProvider extends ConnectionProvider {
  val stompClientOptions: StompClientOptions =
    DetailsConnectionProvider("0.0.0.0", 61613).stompClientOptions.setHeartbeat(noHeartBeatObject)
}

final case class Credentials(
    username: String,
    password: String
) {
  override def toString: String = s"Credentials($username,*****)"
}

final case class DetailsConnectionProvider(
    host: String,
    port: Int,
    credentials: Option[Credentials] = None
) extends ConnectionProvider {

  def stompClientOptions: StompClientOptions = {
    val opt = new StompClientOptions()
    opt.setHost(host)
    opt.setPort(port)
    credentials.foreach { c =>
      opt.setLogin(c.username)
      opt.setPasscode(c.password)
    }
    opt.setHeartbeat(noHeartBeatObject)
    opt
  }
}
