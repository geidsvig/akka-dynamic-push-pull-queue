package geidsvig.akka.queue

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import geidsvig.akka.queue.MemoryActorQueue.{Enqueue, EnqueueRejected, EnqueueSuccess}
import geidsvig.logger.Slf4jLogger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Created by garretteidsvig on 2019-07-16.
  *
  * The adaptor to access memory queue
  *
  * @param actorSystem
  * @param queueRef
  */
class MemoryActorQueueAdaptor(val actorSystem: ActorSystem, queueRef: ActorRef)
  extends QueueAdapter
  with Slf4jLogger {

  implicit val timeout = Timeout(10 seconds)

  override def enqueue(event: Any): Future[Boolean] = {
    queueRef.ask(Enqueue(event)).map {
      case EnqueueSuccess(_) => true
      case EnqueueRejected(_) => false
    }
  }

}
