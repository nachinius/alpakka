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

sealed trait ConnectionProvider {

  import scala.concurrent.duration._

  val atMost = FiniteDuration(1, SECONDS)

  def getFuture: Future[StompClientConnection]

  def get(atMost: Duration = atMost): StompClientConnection = Await.result(getFuture, atMost)

  def getStompClient: StompClient =
    StompClient.create(Vertx.vertx(), new StompClientOptions().setHeartbeat(new JsonObject().put("x", 0).put("y", 0)))

  def release(connection: StompClientConnection) = connection.disconnect()

  def release(connection: StompClient) = connection.close()
}

/**
 * Connects to a local STOMP server at the default port with no password.
 */
case object LocalConnectionProvider extends ConnectionProvider {
  override def getFuture: Future[StompClientConnection] = {
    val promise = Promise[StompClientConnection]()
    StompClient
      .create(Vertx.vertx(), new StompClientOptions().setHeartbeat(new JsonObject().put("x", 0).put("y", 0)))
      .connect(
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

}
