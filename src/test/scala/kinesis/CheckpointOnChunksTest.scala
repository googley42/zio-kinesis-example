package kinesis

import java.util.UUID

import kinesis.TestUtils._
import nl.vroste.zio.kinesis.client.{Client, DynamicConsumer}
import zio._
import zio.blocking.Blocking
import zio.console.Console.Live.console._
import zio.duration._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

object CheckpointOnChunksTest
    extends DefaultRunnableSpec(
      suite("CheckpointOnChunks ZIO Stream Consumer should")(
        testM(
          "consume all records produced on the stream, across multiple shards"
        ) {

          val streamName = streamNamePrefix + UUID.randomUUID().toString
          val applicationName = applicationNamePrefix + UUID
            .randomUUID()
            .toString
          val nrRecords = 100000
          val batchSize = 500
          val nrShards = 32
          (Client.create <* createStream(streamName, nrShards) <* mgdDynamoDbTableCleanUp(
            applicationName
          )).use { client =>
            for {
              runtime <- ZIO.runtime[Blocking with Any]
              refProcessedCount <- Ref.make[Int](0)
              _ <- putStrLn(s"Putting records into Kinesis stream")
              _ <- putRecordsEmitter(streamName, batchSize, nrRecords, client).runDrain.fork
              stream = DynamicConsumer
                .shardedStream(
                  streamName,
                  applicationName = applicationName,
                  deserializer = TestMsgJsonSerde.jsonSerde
                )
              env <- TestUtils.clockWithBlockingM
              _ <- CheckpointOnChunkEndStreamClient
                .consumeStream(refProcessedCount, runtime, stream)
                .provide(env)
                .runDrain
              count <- refProcessedCount.get
              _ <- zio.clock.Clock.Live.clock
                .sleep(10.seconds) // need this when number of shards is high eg 32 and we use `tap(N)`
            } yield assert(count, equalTo(nrRecords))
          }

        }
      ) @@ timeout(1.hour) @@ sequential
    )
