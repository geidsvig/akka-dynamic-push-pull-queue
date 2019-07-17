package geidsvig.akka.queue

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{TestActorRef, TestProbe}
import akka.util.Timeout
import geidsvig.akka.queue.MemoryActorQueue._
import geidsvig.test.TestKitSpec
import org.scalatest.concurrent.{PatienceConfiguration, ScalaFutures}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by garretteidsvig on 2019-07-16.
  */
class MemoryActorQueueTest extends TestKitSpec(ActorSystem("QueueAdapterTest"))
  with ScalaFutures {

  implicit val timeout = PatienceConfiguration.Timeout(3 seconds)
  implicit val askTimeout = Timeout(3 seconds)

  "A MarchActorQueueAdapter" should "enqueue and dequeue events" in {
    val realmId = 0

    Given("an empty queue")
    val actorQueue = TestActorRef(Props(new MemoryActorQueue("MemoryActorQueue")), "test-queue")
    val queue: MemoryActorQueue = actorQueue.underlyingActor
    val adapter = new MemoryActorQueueAdaptor(system, actorQueue)
    assert(queue.memoryQueue.size == 0)


    When("enqueuing multiple events")
    val event1 = Some("event 1")
    val event2 = Some("event 2")
    Then("the queue should contain the events")
    whenReady(for {
      enqueued1 <- adapter.enqueue(event1)
      enqueued2 <- adapter.enqueue(event2)
      result = assert(queue.memoryQueue.size == 2 && queue.workerQueue.size == 0)
    } yield result, timeout) _

    When("dequeuing one event")
    Then("the event should be returned via Dequeue")
    And("the queue should contain one event")
    whenReady(for {
      events <- actorQueue.ask(Dequeue(1)).mapTo[Events]
      queuesize = assert(queue.workerQueue.size == 1)
      result1 = assert(events.events(0) == event1)
      result2 = assert(queue.memoryQueue.size == 1)
    } yield result2) _

    When("dequeuing the last event")
    Then("the event should be returned via Dequeue")
    And("the queue should should be empty")
    whenReady(for {
      events <- actorQueue.ask(Dequeue(1)).mapTo[Events]
      result1 = assert(events.events(0) == event2)
      result2 = assert(queue.memoryQueue.size == 0)
    } yield result2) _

  }

  "A DynamicPushPullQueue" should "have 2 phases" in {

    val queueName = "dynamic-push-pull-queue"

    Given("an empty queue")
    val actorQueue = TestActorRef(Props(new MemoryActorQueue(queueName)), queueName)
    val queue: MemoryActorQueue = actorQueue.underlyingActor
    assert(queue.memoryQueue.size == 0)


    When("enqueuing multiple events")
    queue.memoryQueue.enqueue("1" , "2", "3", "4", "5", "6", "7", "8", "9", "10")
    Then("the queue should contain the events")
    whenReady(for {
      events <- actorQueue.ask(Dequeue(0)).mapTo[Events]
      result = assert(queue.memoryQueue.size == 10 && queue.workerQueue.size == 0)
    } yield result, timeout) _

    When("dequeuing multiple events")
    Then("the events should be returned via Dequeue")
    And("the queue should contain fewer events")
    whenReady(for {
      events <- actorQueue.ask(Dequeue(4)).mapTo[Events]
      queuesize = assert(queue.workerQueue.size == 1)
      result1 = assert(events.events.size == 4)
      result2 = assert(queue.memoryQueue.size == 6)
    } yield result2) _

    When("dequeuing the remaining events")
    Then("the events should be returned via Dequeue")
    And("the queue should should be empty")
    whenReady(for {
      events <- actorQueue.ask(Dequeue(10)).mapTo[Events]
      result1 = assert(events.events.size == 6)
      result2 = assert(queue.memoryQueue.size == 0)
    } yield result2) _

    When("requesting to dequeue when no events are in queue")
    val probe = TestProbe("workerProbe")
    Then("the event list response should be empty")
    And("the queue should should be empty")
    actorQueue.tell(Dequeue(10), probe.ref)

    When("events enqueued while worker is waiting")
    actorQueue.tell(Enqueue("11"), probe.ref)
    Then("the enqueued events should be pushed to waiting worker")
    probe.expectMsg(Events(List("11")))
  }

}

