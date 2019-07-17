package geidsvig.akka.queue

import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.config.ConfigFactory
import geidsvig.logger.Slf4jLogger

import scala.collection.mutable
import scala.util.Try

/**
  * Created by garretteidsvig on 2019-07-16.
  */
object MemoryActorQueue extends Slf4jLogger {
  def prop( memoryQueueName: String) = {
    Props(new MemoryActorQueue(memoryQueueName))
  }

  //requests
  case class Enqueue(event: Any)
  case class Dequeue(batchSize: Int)
  //results
  case class EnqueueSuccess(event: Any)
  case class EnqueueRejected(event: Any)
  case class Events(events: List[Any])

}

/**
  * Memory queue based on Actor
  *
  * Implementation of a dynamic push-pull managed queue.
  *
  * Push state -> work enqueued is pushed round-robin to the next worker.
  * Pull state -> work enqueued is held until worker requests a batch.
  *
  */
class MemoryActorQueue(name: String = "Not-Given")
  extends Actor
  with Slf4jLogger {

  import MemoryActorQueue._

  val config = ConfigFactory.load()

  val highWaterMark = Try(config.getInt("queue.manager.highwatermark")).getOrElse(1000)
  val defaultBatchSize = Try(config.getInt("queue.worker.batchsize")).getOrElse(50)

  val workerQueue = new mutable.Queue[ActorRef]()

  val memoryQueue = new mutable.Queue[Any]()

  override def preStart(): Unit = {
    logger.debug(s"$name started")
    super.preStart()
  }

  override def receive = receiveQueueMessage

  /**
    * Queue messages handler
    * @return
    */
  def receiveQueueMessage : Actor.Receive= {
    case Enqueue(event) => {
      logger.debug(s"$name enqueue requested. current queue size ${memoryQueue.size+1}")

      if (workerQueue.isEmpty) {
        // no registered workers. enqueue task
        // check high water mark. reject if queue is full.
        if (memoryQueue.size < highWaterMark) {
          logger.debug(s"$name no registered workers. queued task")
          memoryQueue.enqueue(event)
          sender ! EnqueueSuccess(event)
        } else {
          logger.warn(s"$name high water mark")
          sender ! EnqueueRejected(event)
        }
      } else {
        // push to idle worker.
        val worker: ActorRef = workerQueue.dequeue()
        memoryQueue.enqueue(event)
        val tasks = dequeue(defaultBatchSize)
        worker ! Events(tasks)
        logger.debug(s"$name pushing ${tasks.size} to idle worker $worker")
        sender ! EnqueueSuccess(event)
      }
    }

    case Dequeue(batchSize) => {
      if (memoryQueue.size > 0) {
        logger.debug(s"$name dequeue requested. current queue size ${memoryQueue.size}")
        val tasks = dequeue(batchSize)
        sender ! Events(tasks)
      } else {
        // no tasks available. enqueue worker request.
        logger.debug(s"$name no tasks available. enqueue worker $sender")
        workerQueue.enqueue(sender)
        logger.debug(s"$name enqueued workers ${workerQueue}")
      }
    }
  }

  /**
    * Takes batchSize number of items from queue.
    *
    * @param batchSize
    * @return
    */
  def dequeue(batchSize: Int): List[Any] = {
    var counter = 0
    memoryQueue.dequeueAll(_ => {
      counter = counter + 1
      counter <= batchSize
    }).toList
  }

}
