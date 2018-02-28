/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.stomp.{Frame, StompClient, StompClientConnection, StompClientOptions}

object ConnectorSettings {

  def apply(): ConnectorSettings =
    ConnectorSettings(connectionProvider = LocalConnectionProvider)
}

case class ConnectorSettings(
    connectionProvider: ConnectionProvider,
//                              destination: Option[String] = None,
//                              requestReceiptHandler: Option[Frame => ()] = None
)

sealed trait ConnectionProvider {
  def get: StompClient

  def release(connection: StompClientConnection) = connection.disconnect()

  def release(connection: StompClient) = connection.close()
}

/**
 * Connects to a local STOMP server at the default port with no password.
 */
case object LocalConnectionProvider extends ConnectionProvider {
  override def get: StompClient =
    StompClient.create(Vertx.vertx(), new StompClientOptions().setHeartbeat(new JsonObject().put("x", 0).put("y", 0)))

  /**
   * Java API
   */
  def getInstance(): LocalConnectionProvider.type = this
}
