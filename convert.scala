package com.toptal.doobie

import doobie.imports._
import scalaz._, Scalaz._
import scalaz.stream._
import scalaz.concurrent.Task
import java.io.FileWriter
import scalaz.stream.Process
import java.sql.Timestamp
import scala.util.control.Breaks._
import scopt._



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

  def performedActionsSelect(offset : Int, limit: Int) =
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
    val writer = new FileWriter(outputFilePath, true)

    def writeRecords(page: Int) : Boolean = {
      val records = performedActionsSelect(page * pageSize, pageSize).list.transact(xa).unsafePerformSync
      records.foreach { x => writer.append(x.toString) }
      records.size > 0
    }
    Range(0, pages).takeWhile(writeRecords)
    writer.close()
  }

  def writeWithStream(outputFilePath: String, pageSize: Int, pages: Int): Unit = {

    performedActionsSelect2(0, pages * pageSize)
      .process
      .evalMap(_.toString)
      .to(io.stdOutLines)
      .transact(xa)
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
