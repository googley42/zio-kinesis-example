package kinesis

import java.util.UUID

import kinesis.TestUtils._
import nl.vroste.zio.kinesis.client.{Client, DynamicConsumer}
import zio._
import zio.console.Console.Live.console._
import zio.duration._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

object FlattenChunksTest
    extends DefaultRunnableSpec(
      suite("ZIO Stream Consumer should")(
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
              refProcessedCount <- Ref.make[Int](0)
              _ <- putStrLn(s"Putting records into Kinesis stream")
              _ <- putRecordsEmitter(streamName, batchSize, nrRecords, client).runDrain.fork
              _ <- DynamicConsumer
                .shardedStream(
                  streamName,
                  applicationName = applicationName,
                  deserializer = TestMsgJsonSerde.jsonSerde
                )
                .flatMapPar(Int.MaxValue)(_._2.flattenChunks)
                .take(nrRecords)
                .tap(
                  r =>
                    for {
                      count <- refProcessedCount.update(count => count + 1)
                      _ <- putStrLn(
                        s"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX count=$count Got record ${r.data}"
                      )
                      _ <- checkpoint(r, refProcessedCount, 500)
                    } yield ()
                )
                .runDrain
              count <- refProcessedCount.get
              // need this when number of shards is high eg 32 and we use `take(N)` to prevent below error:
              // Unexpected exception was thrown. This could probably be an issue or a bug. Please search for the exception/error online to check what is going on. If the issue persists or is a recurring problem, feel free to open an issue on, https://github.com/awslabs/amazon-kinesis-client.
              _ <- zio.clock.Clock.Live.clock
                .sleep(10.seconds)
            } yield assert(count, equalTo(nrRecords))
          }

        }
      ) @@ timeout(30.minute) @@ sequential
    )
