/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp

import io.vertx.ext.stomp.Frame

package object client {

  sealed trait StompThrowable extends Exception

  case class StompProtocolError(frame: Frame) extends StompThrowable {
    override def toString: String = super.toString + frame.toString
  }

  case class StompClientConnectionDropped(str: String = "") extends StompThrowable

}
