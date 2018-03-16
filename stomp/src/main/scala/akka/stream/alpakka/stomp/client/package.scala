package akka.stream.alpakka.stomp

import io.vertx.ext.stomp.Frame

package object client {

  // @TODO: better semantics to this exception
  sealed trait StompThrowable extends Exception

  case class StompProtocolError(frame: Frame) extends StompThrowable {

  }

  case class StompClientConnectionDropped(str: String = "") extends StompThrowable

  case class IncorrectCommand(str: String = "Only SEND command is accepted in StompSink") extends StompThrowable

  case class StompBadReceipt(str: String) extends StompThrowable// for implementing debugging functionality
}
