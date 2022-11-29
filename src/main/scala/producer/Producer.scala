package producer

import akka.actor.{Actor, ActorRef, Props}
import akka.io.{IO, Tcp}
import akka.util.ByteString
import consumer.Message
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import org.json4s.{Formats, NoTypeHints}

import java.net.InetSocketAddress

object Producer {
    def props(remote: InetSocketAddress, replies: ActorRef): Props =
        Props(new Producer(remote, replies))
}

class Producer(remote: InetSocketAddress, listener: ActorRef) extends Actor {
    import akka.io.Tcp._
    import context.system

    IO(Tcp) ! Connect(remote)

    implicit val formats: Formats = Serialization.formats(NoTypeHints)

    def receive: Receive = {
        case CommandFailed(_: Connect) =>
            listener ! "connect failed"
            context.stop(self)
        case c @ Connected(_, _) =>
            println("Producer Address:", self)
            val connection = sender()
            connection ! Register(self)
            listener ! c
            context.become {
                case Message(topic, text, _) =>
                    connection ! Write(ByteString(write(Message(topic, text))))
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