package akka.stream.alpakka.stomp.client

import akka.Done
import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import io.vertx.ext.stomp.{Frame, StompClientConnection}

import scala.concurrent.{Future, Promise}

final class SinkStage(settings: ConnectorSettings)
  extends GraphStageWithMaterializedValue[SinkShape[Frame], Future[Done]] {
  stage =>

  val in = Inlet[Frame]("StompClientSink.in")

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    (new GraphStageLogic(shape) with ConnectorLogic {
      override val settings = stage.settings
      //      private val destination = settings.destination
      //      private val requestReceiptHandler: Option[Frame => ()] = settings.requestReceiptHandler
      private var expectedReceiptId: Option[String] = None

      override def whenConnected: Unit = {
        connection.exceptionHandler(failStage(_))
        connection.closeHandler(_ => cancel(in))
        connection.errorHandler(frame => {
          val ex = StompProtocolError(frame.toString)
          failStage(ex)
        })

        // for implementing debugging functionality
        // connection.writingFrameHandler()

        connection.connectionDroppedHandler(droppedConnection => {
          val ex = StompClientConnectionDropped(droppedConnection.toString)
        })
        connection.receivedFrameHandler(frame => checkForRequestIdIfExpected(frame))
        pull(in)
      }

      val acceptedCommands = Set(Frame.Command.SEND)

      def checkCommand(frame: Frame) = {
        if (!acceptedCommands.contains(frame.getCommand)) {
          failStage(StompSinkStageFailedCommand())
        }
      }

      private def prepareExpectationOnReceipt(originalFrame: Frame) = {
        if (originalFrame.getHeaders.containsKey(Frame.RECEIPT)) {
          expectedReceiptId = Some(originalFrame.getHeader(Frame.RECEIPT))
        }
      }

      def checkForRequestIdIfExpected(frame: Frame) = {
        expectedReceiptId.map {
          expected =>
            if (frame.getCommand == Frame.Command.RECEIPT) {
              if (frame.getHeaders.containsKey(Frame.RECEIPT_ID) && expected == frame.getHeader(Frame.RECEIPT_ID)) {
                expectedReceiptId = None
                Right()
              } else Left("Bad receipt id or missing header")
            } else Left("Not a receipt message")
        } foreach { sol: Either[String, Unit] =>
          sol match {
            case Right(_) => ()
            case Left(str) => failStage(StompBadReceipt(str))
          }
        }
      }

      setHandler(
        in,
        new InHandler {

          override def onUpstreamFailure(ex: Throwable): Unit = {
            promise.failure(ex)
            super.onUpstreamFailure(ex)
          }

          override def onUpstreamFinish(): Unit = {
            promise.success(Done)
            super.onUpstreamFinish()
          }

          override def onPush(): Unit = {
            val originalFrame = grab(in)
            checkCommand(originalFrame)
            prepareExpectationOnReceipt(originalFrame)
            connection.send(originalFrame)
            if (expectedReceiptId.isEmpty) {
              pull(in)
            }
          }
        }
      )


      override def postStop(): Unit = {
        promise.tryFailure(new RuntimeException("stage stopped unexpectedly"))
        super.postStop()
      }

      override def onFailure(ex: Throwable): Unit =
        promise.tryFailure(ex)

    }, promise.future)
  }

  override def shape: SinkShape[Frame] = SinkShape.of(in)

  override def toString: String = "StompClientSink"

  override protected def initialAttributes: Attributes = Attributes.name("StompClientSink")

}

case class StompProtocolError(str: String) extends Throwable

case class StompClientConnectionDropped(str: String = "") extends Throwable

case class StompSinkStageFailedCommand(str: String = "Only SEND command is accepted in StompSink") extends Throwable

case class StompSinkUnexpectedReceiptId(str: String = "") extends Throwable

case class StompUnexpectedCommandWhileWaitingForReceiptId(str: String = "") extends Throwable

case class StompBadReceipt(str: String) extends Throwable