package tc.meetup

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.{Properties, UUID}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object Main {

  case class Meetup(id: UUID, title: String, summary: Option[String], attendees: Integer, pizzas: Option[Integer])

  case class NeedsSummary(id: UUID, meetupId: UUID)

  def main(args: Array[String]): Unit = {
    System.out.println("Starting meetup checker")

    var conn: Connection = null
    try {
      conn = getConnection
      // Note that below is the core logic of our program. No longer polluted with JDBC specific logic.
      val result = getMeetups(conn).flatMap(meetups => {
        meetups.map(meetup => {
          (meetup.summary, meetup.pizzas) match {
            case (None, _) =>
              notifyThatWeNeedSummary(UUID.randomUUID(), meetup.id, conn)
            case (_, None) =>
              updatePizzaCount(meetup.id, meetup.attendees / 3, conn)
            case _ => Success(0)
          }
        // It's unfortunate that we have to do this fold. We want to squash our List[Try[Int]] into
        // a Try[List[Int]]. Often, libraries provide utilities for this.
        }).fold(Success(0)) { (m1: Try[Int], m2: Try[Int]) =>
          m1.flatMap(r1 => m2.map(r2 => r1 + r2))
        }
      })

      System.out.println(s"""Updated $result rows""")

    } finally {
      if (conn != null) conn.close()
    }

    System.out.println("Ending meetup checker")
  }

  def getConnection: Connection = {
    val url = "jdbc:postgresql://localhost:15432/tcscala"
    val props: Properties = new Properties()
    props.setProperty("user", "admin")
    props.setProperty("password", "admin")
    DriverManager.getConnection(url, props)
  }

  // We wrap our exception throwing code in Try as a quick way to make our functions total.
  // We have to use Try because of the exceptions thrown by JDBC. Some programs might also introduce
  // their own errors. In a perfect world we treat our errors as data and in a type hierarchy that
  // we control. Either or something like it might be a good choice. For brevity, we will stick with
  // Try here.
  def getMeetups(conn: Connection): Try[List[Meetup]] = {
    var selectAllStatement: PreparedStatement = null
    var selectAllResults: ResultSet = null
    val meetups = Try {
      selectAllStatement = conn.prepareStatement(Sql.SelectAll)
      selectAllResults = selectAllStatement.executeQuery()
      selectAllResults
    } flatMap (fromResultSet)

    if (selectAllStatement != null) selectAllStatement.close()
    if (selectAllResults != null) selectAllResults.close()

    meetups
  }

  def toMeetup(resultSet: ResultSet): Try[Meetup] = {
    Try {
      val id = resultSet.getObject(Columns.Id, classOf[UUID])
      val title = resultSet.getString(Columns.Title)
      val summary = Option(resultSet.getString(Columns.Summary))
      val attendees = resultSet.getInt(Columns.Attendees)
      val pizzas = Option(resultSet.getObject(Columns.Pizzas, classOf[Integer]))
      Meetup(id, title, summary, attendees, pizzas)
    }
  }

  def fromResultSet(resultSet: ResultSet): Try[List[Meetup]] = {
    @tailrec
    def helper(meetupResult: Try[List[Meetup]], resultSet: ResultSet): Try[List[Meetup]] = {
      meetupResult match {
        case Failure(error) => Failure(error)
        case Success(meetups) =>
          Try(resultSet.next()) match {
            case Success(true) => helper(toMeetup(resultSet).map(meetup => meetups :+ meetup), resultSet)
            case Success(false) => Success(meetups)
            case Failure(error) => Failure(error)
          }
      }
    }

    helper(Success(List()), resultSet)
  }

  def updatePizzaCount(id: UUID, numberOfPizzas: Int, conn: Connection): Try[Int] = {
    var updateStatement: PreparedStatement = null
    val rows = Try {
      updateStatement = conn.prepareStatement(Sql.UpdatePizzas)
      updateStatement.setInt(1, numberOfPizzas)
      updateStatement.setObject(2, id)
      updateStatement.executeUpdate()
    }

    Try {
      if (updateStatement != null) updateStatement.close()
    } flatMap(_ => rows)
  }

  def notifyThatWeNeedSummary(id: UUID, meetupId: UUID, conn: Connection): Try[Int] = {
    var insertStatement: PreparedStatement = null
    val rows = Try {
      insertStatement = conn.prepareStatement(Sql.CreateNeedsSummary)
      insertStatement.setObject(1, id)
      insertStatement.setObject(2, meetupId)
      insertStatement.executeUpdate()
    }

    Try {
      if (insertStatement != null) insertStatement.close()
    } flatMap(_ => rows)
  }

  object Sql {
    val SelectAll = "select id, title, summary, attendees, pizzas from meetup"
    val UpdatePizzas = "update meetup set pizzas = ? where id = ?"
    val CreateNeedsSummary = "insert into needs_summary (id, meetup_id) values (?, ?)"
  }

  object Columns {
    val Id = "id"
    val Title = "title"
    val Summary = "summary"
    val Attendees = "attendees"
    val Pizzas = "pizzas"
  }

}
