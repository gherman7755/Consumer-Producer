package producer

import akka.actor.Actor
import akka.io.Tcp.Connected
import consumer.{Message, SendCreate}
import main.main.{producer, producerListener, system, topicList}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import main.main.system.scheduler
import main.main.system.dispatcher

import scala.concurrent.duration.DurationInt
import scala.util.Random


class ProducerListener extends Actor{
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    println(s"Producer listener: $self")

    def receive: Receive = {
        case SendCreate(_, producer, _) =>
            def randomString(length: Int) = {
                val r = new Random
                val sb = new StringBuilder
                for (_ <- 1 to length){
                    sb.append(r.nextPrintableChar)
                }
                sb.toString
            }

            val createdMessage = Message(topicList(Random.nextInt(topicList.length)), randomString(Random.nextInt(25)))
            producer ! createdMessage
        case _: Connected =>
            scheduler.scheduleAtFixedRate(10.millis, 100.millis, producerListener, SendCreate("create message", producer))
    }
}
