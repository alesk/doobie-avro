import java.sql.Timestamp
import doobie.imports._
import scalaz.concurrent.Task
import scalaz.stream.{Process, _}

case class PerformedAction(
  id: Int,
  subject_id: Int,
  subject_type: String,
  performer_id: Option[Int],
  created_at: Timestamp,
  updated_at: Timestamp,
  operand_id: Option[Int],
  action: String,
  operand_type: Option[String],
  specification: Option[String],
  technical_details: Option[String])

object DoobieAvro {
  case class Args(
    streaming : Boolean = false,
    pageSize: Int = 10000,
    pages: Int = Int.MaxValue,
    out : String = "performed_actions.txt"
  )

  val xa = DriverManagerTransactor[Task]("org.postgresql.Driver", "jdbc:postgresql:toptal_development", "toptal", "")

  def performedActionsSelect(offset : Int, limit: Int): Query0[PerformedAction] =
    sql"""
    select id,
    subject_id,
    subject_type,
    performer_id,
    created_at,
    updated_at,
    operand_id,
    action,
    operand_type,
    specification,
    technical_details
    from performed_actions where subject_id IS NOT NULL
    offset $offset limit $limit
    """.query[PerformedAction]

  def batchRead(offset: Int, limit: Int): Task[Vector[PerformedAction]] =
  {
    println(s"Got batch (offset: $offset)")
    performedActionsSelect(offset, limit).vector.transact(xa)
  }

  def batchReadProcess(start: Int, batchSize: Int): Process[Task, PerformedAction] = {
    Process.await(batchRead(start, batchSize)) { els =>
      if (els.isEmpty) Process.halt
      else Process.emitAll(els) ++ batchReadProcess(start + els.size, batchSize)
    }
  }


  def performedActionsSelect2(offset : Int, limit: Int) =
    sql"""
    select id,
    subject_id,
    subject_type,
    performer_id,
    created_at,
    updated_at,
    operand_id,
    action,
    operand_type,
    specification,
    technical_details
    from performed_actions where subject_id IS NOT NULL
    """.query[PerformedAction]

  def writeInChunks(outputFilePath: String, pageSize : Int, pages : Int): Unit = {
    batchReadProcess(0, 1024 * 100)
      .map(_.toString)
      .pipe(text.utf8Encode)
      .to(io.fileChunkW("/tmp/perf_actions.txt", 10 * 1024 * 1024))
      .run
      .run
  }

  def writeWithStream(outputFilePath: String, pageSize: Int, pages: Int): Unit = {

    val proc: Process[ConnectionIO, PerformedAction] = performedActionsSelect(0, pages * pageSize)
      .process

    proc
      .transact(xa)
      .map(_.toString)
      .to(io.stdOutLines)
      .run
      .run

    println("Streaming finished")
  }

  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[Args]("doobie-avro") {
      opt[String]("out").optional().action {(x, c) => c.copy(out = x)}
      opt[Boolean]("streaming").optional().action {(x, c) => c.copy(streaming = x)}
      opt[Int]("page-size").optional().action {(x, c) => c.copy(pageSize = x)}
      opt[Int]("pages").optional().action {(x, c) => c.copy(pages = x)}
    }

    parser.parse(args, Args()).foreach { args =>
      if (args.streaming) {
        writeWithStream(args.out, args.pageSize, args.pages)
      } else {
        writeInChunks(args.out, args.pageSize, args.pages)
      }
    }
  }
}
