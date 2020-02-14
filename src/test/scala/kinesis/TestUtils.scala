package kinesis

import java.net.URI

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.DynamicConsumer.Record
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.{AdminClient, Client}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.kinesis.model.{
  ResourceInUseException,
  ResourceNotFoundException
}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.stream.ZStream
import zio._
import zio.duration._

object TestUtils {

  val streamNamePrefix = "mercury-invoice-generator-dev"
  val applicationNamePrefix = "mercury-invoice-generator-zio-test-"

  def recordAsJsonString(i: Int) = s"""{ "id": $i }"""

  val retryOnResourceNotFound = Schedule.doWhile[Throwable] {
    case _: ResourceNotFoundException => true
    case _                            => false
  } &&
    Schedule.recurs(5) &&
    Schedule.exponential(2.second)
  println(retryOnResourceNotFound)

  val createStream = (streamName: String, nrShards: Int) =>
    for {
      adminClient <- AdminClient.create
      _ <- adminClient
        .createStream(streamName, nrShards)
        .catchSome {
          case _: ResourceInUseException =>
            println("Stream already exists")
            ZIO.unit
        }
        .toManaged { _ =>
          adminClient
            .deleteStream(streamName, enforceConsumerDeletion = true)
            .catchSome {
              case _: ResourceNotFoundException => ZIO.unit
            }
            .orDie
        }
    } yield ()

  def putRecordsEmitter(streamName: String,
                        batchSize: Int,
                        max: Int,
                        client: Client) =
    ZStream.unfoldM(1) { i =>
      if (i < max) {
        val recordsBatch = (i to i + batchSize - 1)
          .map(i => ProducerRecord(s"key$i", recordAsJsonString(i)))
        val putRecordsM = client
          .putRecords(streamName, Serde.asciiString, recordsBatch)
          .retry(retryOnResourceNotFound)
          .provide(Clock.Live)
        zio.console.Console.Live.console
          .putStrLn(s"i=$i putting $batchSize  records into Kinesis") *>
          zio.clock.Clock.Live.clock.sleep(500.milliseconds) *>
          putRecordsM *>
          ZIO.effectTotal(Some((i, i + batchSize)))
      } else {
        ZIO.effectTotal(None)
      }
    }

  // TODO: read config
  private val endpoint = URI.create("https://dynamodb.us-east-1.amazonaws.com")
  private val region = Region.US_EAST_1

  def mgdDynamoDbTableCleanUp(
    appName: String
  ): ZManaged[Any, Nothing, DynamoDbAsyncClient] = {
    import zio.interop.javaz._
    ZManaged.make(
      UIO(
        DynamoDbAsyncClient
          .builder()
          .endpointOverride(endpoint)
          .region(region)
          .build()
      )
    ) { client =>
      val delete = ZIO.effectTotal(
        client
          .deleteTable(DeleteTableRequest.builder.tableName(appName).build())
      )
      ZIO.fromCompletionStage(delete).ignore *> ZIO
        .effect(client.close())
        .ignore
    }
  }

  def checkpoint[T](r: Record[T],
                    ref: Ref[Int],
                    batchSize: Int): ZIO[Clock with Blocking, Throwable, Unit] =
    ref.get.flatMap { count =>
      if (count % batchSize == 0)
        r.checkpoint.retry(Schedule.exponential(100.millis))
      else ZIO.unit
    }

}
