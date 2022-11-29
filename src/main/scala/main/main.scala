package main

import akka.actor.{ActorSystem, Props}
import akka.io.Tcp.Received
import akka.util.ByteString
import consumer.{Consumer, ConsumerListener, SendCreate, SubscribeFromMain, UnsubscribeFromMain}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization
import producer.{Producer, ProducerListener}

import java.net.InetSocketAddress
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt
import scala.util.Random

object main extends App {
    implicit val system: ActorSystem = ActorSystem()
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    implicit val executor: ExecutionContextExecutor = system.dispatcher

    val topicList = List("Technology", "Computer", "Science", "Newspaper", "Family", "Pollution", "Nature", "Junk Food")

    val consumerListener = system.actorOf(Props(classOf[ConsumerListener]))
    val consumer = system.actorOf(Consumer.props(new InetSocketAddress("localhost", 8080), consumerListener))

    val consumerListener1 = system.actorOf(Props(classOf[ConsumerListener]))
    val consumer1 = system.actorOf(Consumer.props(new InetSocketAddress("localhost", 8080), consumerListener1))

    val producerListener = system.actorOf(Props(classOf[ProducerListener]))
    val producer = system.actorOf(Producer.props(new InetSocketAddress("localhost", 8080), producerListener))

    system.scheduler.scheduleAtFixedRate(500.millis, 5.seconds)(() => {
        val choose = List("sub", "uns")
        if (choose(Random.nextInt(choose.length)) == "sub") {
            consumer ! Received(ByteString(Serialization.write(SubscribeFromMain())))
            consumer1 ! Received(ByteString(Serialization.write(SubscribeFromMain())))

        } else {
            consumer ! Received(ByteString(Serialization.write(UnsubscribeFromMain())))
            consumer1 ! Received(ByteString(Serialization.write(SubscribeFromMain())))

        }
    })
}
