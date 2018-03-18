package akka.stream.alpakka.stomp.client

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.stream.ActorMaterializer
import io.vertx.core.{Vertx, VertxOptions}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpec}
import scala.concurrent.duration._

trait ClientTest extends WordSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with ScalaFutures {

  implicit val system = ActorSystem(this.getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()(system)

  val vertx: Vertx = Vertx.vertx(new VertxOptions().setBlockedThreadCheckInterval(10))

  override implicit val patienceConfig = PatienceConfig(1.seconds)
  implicit val executionContext = ExecutionContexts.sameThreadExecutionContext

  override def afterAll(): Unit = {
    system.terminate()
    vertx.close()
    super.afterAll()
  }
}
