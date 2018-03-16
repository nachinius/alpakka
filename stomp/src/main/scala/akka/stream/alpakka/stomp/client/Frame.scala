package akka.stream.alpakka.stomp.client

case class Frame2(command: Command, headers: Map[String,String], body: Vector[Byte]) {

}

sealed trait Command
case object SEND extends Command


trait Header
