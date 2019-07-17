package geidsvig.akka.queue

import scala.concurrent.Future

/**
  * Created by garretteidsvig on 2019-07-16.
  */
object QueueAdapter {
  case class Dequeued(events: List[Any])
}

/**
  * Created by garretteidsvig on 2019-07-16.
  *
  * Adapter to access queue
  */
trait QueueAdapter {
  def enqueue(event: Any): Future[Boolean]
}
