package com.jambit.stream.detector

import com.jambit.stream.detector.MatchTypes.{ MatchEvent, Message }
import org.apache.flink.api.common.eventtime.{ SerializableTimestampAssigner, WatermarkStrategy }
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.{ CEP, PatternStream }
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.time.Duration

object MatchDetectorApp {
  def createMatchEventStream(
      env: StreamExecutionEnvironment,
      input: DataStream[Message]
  ): DataStream[MatchEvent] = {
    // 1) Define the pattern: start -> end
    val startEndPattern: Pattern[Message, Message] = Pattern
      .begin[Message]("start")
      .where((msg, _) => msg.messageType == "start")
      .followedBy("end")
      .where((msg, _) => msg.messageType == "end")

    // 2) Key by userId and apply the pattern
    val keyedStream                           = input.keyBy(_.userId)
    val patternStream: PatternStream[Message] = CEP.pattern(keyedStream, startEndPattern)

    // 3) Use .select to create a MatchEvent
    patternStream.select { patternMap =>
      val startMsg = patternMap.get("start").get(0)
      val endMsg   = patternMap.get("end").get(0)
      new MatchEvent(
        userId        = startMsg.userId,
        experienceWon = endMsg.messageValue - startMsg.messageValue
      )
    }
  }

  def detect(): Unit = {
    val executionConfiguration = new Configuration()
    val env = StreamExecutionEnvironment.getExecutionEnvironment(executionConfiguration)

    env.getConfig.setAutoWatermarkInterval(1_000)
    env.enableCheckpointing(5_000)

    val kafkaSource = KafkaSource
      .builder[Message]
      .setBootstrapServers("kafka-a:29092")
      .setTopics("test")
      .setGroupId("match-detector")
      .setStartingOffsets(OffsetsInitializer.earliest)
      .setValueOnlyDeserializer(new MessageDeserializer)
      .build

    val watermarkStrategy = WatermarkStrategy
      .forBoundedOutOfOrderness(Duration.ofSeconds(1))
      .withTimestampAssigner(new SerializableTimestampAssigner[Message] {
        override def extractTimestamp(element: Message, recordTimestamp: Long): Long =
          element.timeOfReceipt
      })
      // exclude sources from watermarking which do not receive messages for more than 5s
      .withIdleness(Duration.ofSeconds(5))

    val eventStream: DataStream[Message] = env
      .fromSource(kafkaSource, watermarkStrategy, "Kafka Source")
      .uid("kafka-source-id")

    val matchEvents = createMatchEventStream(env, eventStream)

    matchEvents.print().uid("print-sink-id")
    env.execute()
  }

  def main(args: Array[String]): Unit =
    detect()
}
