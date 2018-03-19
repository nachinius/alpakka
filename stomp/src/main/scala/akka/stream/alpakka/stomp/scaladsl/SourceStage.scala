/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.scaladsl

import akka.Done
import akka.stream.alpakka.stomp.client.{ConnectorSettings, SendingFrame, SourceStage}
import akka.stream.scaladsl.Source

import scala.concurrent.Future

object SourceStage {

  /**
   * Scala API:
   *
   * Uppon materialization connects and subscribs to a topic (set in settings) published in a Stomp server. Ack each message upon backpressure.
   */
  def apply(settings: ConnectorSettings): Source[SendingFrame, Future[Done]] =
    Source.fromGraph(new SourceStage(settings))

}
