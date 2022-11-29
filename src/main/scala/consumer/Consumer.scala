package consumer

import akka.actor.{Actor, ActorRef, Props}
import akka.io.{IO, Tcp}
import akka.util.ByteString
import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.Serialization.{read, write}
import akka.pattern.ask

import java.net.InetSocketAddress
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

case class ReceivedMessage(data: ByteString, caseType: String="ReceivedMessage")
case class GetTopicList(request: String="get", caseType: String="GetTopicList")
case class SubscribeMessage(request: String="sub", topics: List[String], caseType: String="SubscribeMessage")
case class UnsubscribeMessage(request: String="uns", caseType: String="UnsubscribeMessage")
case class ReceiveTopics(data: Set[String], caseType: String="ReceiveTopics")
case class ReceiveSubscribedTopics(topics: List[String], caseType: String="ReceiveSubscribedTopics")
case class ReceiveUnsubscribeTopic(data: String, caseType: String="ReceiveUnsubscribeTopic")
case class SendCreate(request: String="create message", producer: ActorRef, caseType: String="SendCreate")
case class Message(topic: String, text: String, caseType: String="Message")
case class SubscribeFromMain(request: String="sub", caseType: String="SubscribeFromMain")
case class UnsubscribeFromMain(request: String="uns", caseType: String="UnsubscribeFromMain")
case class Subscription(request: String = "sub", topics: List[String], caseType: String="Subscription")
case class Unsubscribe(request: String = "un", topics: List[String], caseType: String="Unsubscribe")
case class RequestTopics(request: String = "get", caseType: String="RequestTopics")
case class SendTopics(topics: Set[String], caseType: String="SendTopics")

object Consumer {
    def props(remote: InetSocketAddress, replies: ActorRef): Props =
        Props(new Consumer(remote, replies))
}

class Consumer(remote: InetSocketAddress, listener: ActorRef) extends Actor {
    import akka.io.Tcp._
    import context.system

    implicit val formats: Formats = Serialization.formats(NoTypeHints)

    IO(Tcp) ! Connect(remote)

    def receive: Receive = {
        case CommandFailed(_: Connect) =>
            listener ! "connect failed"
            context.stop(self)
        case c @ Connected(_, _) =>
            println(s"Consumer Address=$self")
            val connection = sender()
            connection ! Register(self)
            context.become {
                case Received(data) =>
                    val json = JsonMethods.parse(data.utf8String)
                    (json \ "caseType").extract[String] match {
                        case "Message" =>
                            listener ! ReceivedMessage(data)
                        case "SendTopics" =>
                            val topics = (json \ "topics").extract[Set[String]]
                            listener ! SubscribeMessage(topics=topics.toList)
                        case "SubscribeFromMain" =>
                            val send = Write(ByteString(write(GetTopicList())))
                            connection ! send
                        case "UnsubscribeFromMain" =>
                            listener ! UnsubscribeMessage
                        case "ReceiveSubscribedTopics" =>
                            val topics = (json \ "topics").extract[List[String]]
                            connection ! Write(ByteString(write(Subscription(topics = topics))))
                        case "ReceiveUnsubscribeTopic" =>
                            val topic = (json \ "data").extract[String]
                            connection ! Write(ByteString(write(Unsubscribe(topics=List(topic)))))
                        case "CommandFailed" =>
                            listener ! "write failed"
                        case "close" =>
                            connection ! Close
                    }
                case CommandFailed(_: Write) =>
                    listener ! "write failed"
                case "close" =>
                    connection ! Close
                case _: ConnectionClosed =>
                    listener ! "connection closed"
                    context.stop(self)
            }
    }
}
