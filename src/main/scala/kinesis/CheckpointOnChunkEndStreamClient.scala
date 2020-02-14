package kinesis

import nl.vroste.zio.kinesis.client.DynamicConsumer
import nl.vroste.zio.kinesis.client.DynamicConsumer.Record
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console.Live.console._
import zio.duration._
import zio.logging.Logging
import zio.stream.{ZSink, ZStream, ZStreamChunk}
import zio.{Chunk, IO, Ref, Runtime, Schedule, UIO, ZIO}
import zio.logging._

object CheckpointOnChunkEndStreamClient {

  def consumeStream[T](
    refProcessedCount: Ref[Int],
    runtime: Runtime[Blocking with Any],
    stream: ZStream[Blocking with Any,
                    Throwable,
                    (String,
                     ZStreamChunk[Any, Throwable, DynamicConsumer.Record[T]])]
  ): ZStream[Logging with Clock with Blocking with Any, Throwable, Unit] = {
    val outerStream =
      stream.flatMapPar(Int.MaxValue) { t =>
        val (
          shardName: String,
          chunks: ZStreamChunk[Any, Throwable, DynamicConsumer.Record[T]]
        ) = t

        def checkpoint(ref: Ref[Option[Record[T]]]) =
          for {
            maybeLastOk <- ref.get
            _ <- maybeLastOk
              .fold[ZIO[Logging with Clock with Blocking, Nothing, Unit]](
                UIO.unit
              )(
                rec =>
                  info(s">>>> checkpointing ${rec.data}") *>
                    rec.checkpoint
                      .retry(Schedule.exponential(100.millis))
                      .ignore
              )
          } yield ()

        def processChunk(chunkIndex: Int, chunk: Chunk[Record[T]]) =
          for {
            fiberId <- ZIO.fiberId
            _ <- info(
              s"fiberID=$fiberId shard $shardName, about to process chunk $chunkIndex"
            )
            maybeLastProcessed <- Ref
              .make[Option[Record[T]]](None) // create a new Ref to track last successfully processed record for this chunk
            count <- chunk
              .foldM[Logging with Blocking with Any, Throwable, Int](0) {
                (i, record) => // process each record in chunk
                  for {
                    _ <- processRecord(record.data, refProcessedCount) // we ensure that we do processRecord and update of Ref as an atomic operation
                      .bracket(
                        _ => maybeLastProcessed.update(_ => Some(record))
                      )(_ => ZIO.unit)
                    count = i + 1
                    _ <- info(
                      s">>>> fiberID=$fiberId processed shard $shardName, record number $count, record = $record"
                    )
                  } yield count
              }
              .ensuring(
                info(s">>>> calling finalizer for $fiberId") *> checkpoint(
                  maybeLastProcessed
                )
              )
          } yield count

        val program = for {
          fiberId <- ZIO.fiberId
          _ <- info(
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

  def info(s: String) = {
    log(LogLevel.Info)(s)
  }
}
