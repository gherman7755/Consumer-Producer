package consumer

import akka.actor.Actor
import akka.io.Tcp.Received
import akka.util.ByteString
import org.json4s.jackson.Serialization.write
import org.json4s.native.JsonMethods
import main.main.{system, topicList}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

import scala.collection.mutable.ListBuffer
import scala.util.Random

class ConsumerListener extends Actor{
    var subscribedTopics: Set[String] = Set()
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    println(s"Consumer listener: $self")

    override def receive: Receive = {
        case ReceivedMessage(data, _) =>
            val decodedMessage = JsonMethods.parse(data.utf8String)
            val topicName = (decodedMessage \ "topic").extract[String]
            if (!subscribedTopics(topicName)){
                println(s"Get wrong topic: [$topicName]")
            }
            val text = (decodedMessage \ "text").extract[String]

            println(s"Subscribed Topics: $subscribedTopics")
            println(s"Actor: ${sender()} | Received Topic: $topicName | Message: $text")

        case SubscribeMessage(_, topics, _) =>
            val choice = topicList(Random.nextInt(topicList.length))
            subscribedTopics += choice
            sender() ! Received(ByteString(write(ReceiveSubscribedTopics(subscribedTopics.toList))))

        case UnsubscribeMessage =>
            if(subscribedTopics.nonEmpty) {
                val choice = Random.nextInt(subscribedTopics.size)
                val element = subscribedTopics.iterator.drop(choice).next
                subscribedTopics -= element
                sender() ! Received(ByteString(write(ReceiveUnsubscribeTopic(element))))
            }
    }
}
