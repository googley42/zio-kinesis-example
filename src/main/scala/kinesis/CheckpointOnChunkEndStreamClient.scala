package kinesis

import nl.vroste.zio.kinesis.client.DynamicConsumer
import nl.vroste.zio.kinesis.client.DynamicConsumer.Record
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console.Live.console._
import zio.duration._
import zio.stream.{ZSink, ZStream, ZStreamChunk}
import zio.{Chunk, IO, Ref, Runtime, Schedule, UIO, ZIO}

object CheckpointOnChunkEndStreamClient {

  def consumeStream[T](
    refProcessedCount: Ref[Int],
    runtime: Runtime[Blocking with Any],
    stream: ZStream[Blocking with Any,
                    Throwable,
                    (String,
                     ZStreamChunk[Any, Throwable, DynamicConsumer.Record[T]])]
  ) = {
    val outerStream: ZStream[Blocking with Any, Throwable, Unit] =
      stream.flatMapPar(Int.MaxValue) { t =>
        val (
          shardName: String,
          chunks: ZStreamChunk[Any, Throwable, DynamicConsumer.Record[T]]
        ) = t

        val clockWithBlocking = new Clock.Live with zio.blocking.Blocking {
          override val blocking: Blocking.Service[Any] =
            runtime.environment.blocking
        }

        def checkpoint(ref: Ref[Option[Record[T]]]) =
          for {
            maybeLastOk <- ref.get
            _ <- maybeLastOk.fold[UIO[Unit]](UIO.unit)(
              rec =>
                log(s">>>> checkpointing ${rec.data}") *>
                  rec.checkpoint
                    .retry(Schedule.exponential(100.millis))
                    .ignore
                    .provide(clockWithBlocking)
            )
          } yield ()

        def processChunk(chunkIndex: Int, chunk: Chunk[Record[T]]) =
          for {
            fiberId <- ZIO.fiberId
            _ <- log(
              s"fiberID=$fiberId shard $shardName, about to process chunk $chunkIndex"
            )
            ref <- Ref
              .make[Option[Record[T]]](None) // create a new Ref to track last successfully processed record for this chunk
            count <- chunk
              .foldM[Blocking with Any, Throwable, Int](0) { (i, record) => // process each record in chunk
                for {
                  _ <- processRecord(record.data, refProcessedCount)
                    .bracket(_ => ref.update(_ => Some(record)))(_ => ZIO.unit)
                  count = i + 1
                  _ <- log(
                    s">>>> fiberID=$fiberId processed shard $shardName, record number $count, record = $record"
                  )
                } yield count
              }
              .ensuring(
                log(s">>>> calling finalizer for $fiberId") *> checkpoint(ref)
              )
          } yield count

        val program: ZIO[Blocking with Any, Throwable, Unit] = for {
          fiberId <- ZIO.fiberId
          _ <- log(
            s"shard $shardName, about to process records, fiberId $fiberId"
          )
          _ <- chunks
            .run( // for all chunks
              ZSink.foldLeftM(0)( // foldLeft ensures we are doing constant memory space processing
                processChunk
              )
            )

        } yield ()

        ZStream.fromEffect(program)
      } // end flatMapPar

    outerStream
  }

  def processRecord[T](data: T,
                       refProcessedCount: Ref[Int]): IO[Throwable, Unit] = {
    for {
      count <- refProcessedCount.update(_ + 1)
      _ <- putStrLn(s"processed record $count data=$data")
    } yield ()
  }

  def log(s: String) = putStrLn(s)
}
