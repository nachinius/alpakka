package akka.stream.alpakka.stomp.client

import io.vertx.core.buffer.{Buffer => VertxBuffer}
import io.vertx.ext.stomp.Frame

case class SendingFrame(headers: Map[String,String], body: Vector[Byte]) {

  import scala.collection.JavaConverters._
  def toVertexFrame: Frame = {
    println(body.toArray.toString)
    new Frame().setCommand(Frame.Command.SEND).setHeaders(headers.asJava).setBody(VertxBuffer.buffer(body.toArray))
  }
}

object SendingFrame {
  import scala.collection.JavaConverters._
  def from(frame: Frame): SendingFrame = {
    SendingFrame(frame.getHeaders.asScala.toMap, frame.getBodyAsByteArray.toVector)
  }
}


