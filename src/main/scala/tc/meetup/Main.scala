package tc.meetup

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.{Properties, UUID}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object Main {

  case class Meetup(id: UUID, title: String, summary: Option[String], attendees: Integer, pizzas: Option[Integer])

  case class NeedsSummary(id: UUID, meetupId: UUID)

  case class StatementIO[A](unsafeRun: Connection => A) { self =>

    // Given a way to get from A to B, return a StatementIO of B.
    def map[B](f: A => B): StatementIO[B] = StatementIO((conn: Connection) => f(self.unsafeRun(conn)))

    // Given a way to get from A to StatementIO of B, return a different StatementIO of B.
    def flatMap[B](f: A => StatementIO[B]): StatementIO[B] = StatementIO((conn: Connection) => f(self.unsafeRun(conn)).unsafeRun(conn))

    def list[B](query: String, f: ResultSet => Try[List[B]]): StatementIO[Try[List[B]]] =
      StatementIO((conn: Connection) => listHelper(query, f)(conn))

    def update(query: String, f: PreparedStatement => Try[PreparedStatement]): StatementIO[Try[Int]] =
      StatementIO((conn: Connection) => updateHelper(query, f)(conn))
  }

  object StatementIO {
    def point[A](a: A): StatementIO[A] = StatementIO((conn: Connection) => a)
  }


  def main(args: Array[String]): Unit = {
    System.out.println("Starting meetup checker")

    val io: StatementIO[Try[Int]] = StatementIO.point(())
      .list(Sql.SelectAll, fromResultSet)
      .flatMap {
        // This is tricky because we need to flatMap StatementIO[Try[A]] to StatementIO[Try[B]].
        // Look into Monad Transformers for a better way to do this.
        case Failure(error) => StatementIO.point(Failure(error))
        case Success(meetups: List[Meetup]) =>
          meetups.map(meetup => {
            (meetup.summary, meetup.pizzas) match {
              case (None, _) =>
                StatementIO.point(()).update(Sql.CreateNeedsSummary, setSummaryNotification(UUID.randomUUID(), meetup.id))
              case (_, None) =>
                StatementIO.point(()).update(Sql.UpdatePizzas, setPizzaCount(meetup.id, meetup.attendees / 3))
              case _ =>
                StatementIO.point[Try[Int]](Success(0))
            }
          }).fold(StatementIO.point[Try[Int]](Success(0))) { (m1, m2) =>
            m1.flatMap(e1 => m2.map(e2 => e1.flatMap(r1 => e2.map(r2 => r1 + r2))))
          }
      }

    // The val io is just a bundle of functions that have been combined. if we stopped the program
    // here no JDBC actions will be executes. We must call unsafeExecute to invoke any database calls.
    // Therefore, we have successfully pushed all of our external effects to the very edge of the
    // applications. We can now say we have achieved purity with the above code.

    val result = unsafeExecute(io)

    System.out.println(s"""IO: $io""")
    System.out.println(s"""Result: $result""")

    System.out.println("Ending meetup checker")
  }

  def listHelper[B](query: String, f: ResultSet => Try[List[B]])(conn: Connection): Try[List[B]] = {
    var selectAllStatement: PreparedStatement = null
    var selectAllResults: ResultSet = null
    val results = Try {
      selectAllStatement = conn.prepareStatement(query)
      selectAllResults = selectAllStatement.executeQuery()
      selectAllResults
    } flatMap (f)

    if (selectAllStatement != null) selectAllStatement.close()
    if (selectAllResults != null) selectAllResults.close()

    results
  }

  def updateHelper(query: String, f: PreparedStatement => Try[PreparedStatement])(conn: Connection): Try[Int] = {
    var updateStatement: PreparedStatement = null
    val rows = Try {
      updateStatement = conn.prepareStatement(query)
      updateStatement
    } flatMap { statement =>
      f(statement).map(stmt => stmt.executeUpdate())
    }

    Try {
      if (updateStatement != null) updateStatement.close()
    } flatMap (_ => rows)
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

  def setPizzaCount(id: UUID, numberOfPizzas: Int)(statement: PreparedStatement): Try[PreparedStatement] = {
    Try {
      statement.setInt(1, numberOfPizzas)
      statement.setObject(2, id)
      statement
    }
  }

  def setSummaryNotification(id: UUID, meetupId: UUID)(statement: PreparedStatement): Try[PreparedStatement] = {
    Try {
      statement.setObject(1, id)
      statement.setObject(2, meetupId)
      statement
    }
  }

  def unsafeExecute(io: StatementIO[Try[Int]]): Try[Int] = {
    val url = "jdbc:postgresql://localhost:15432/tcscala"
    val props: Properties = new Properties()
    props.setProperty("user", "admin")
    props.setProperty("password", "admin")
    val conn = DriverManager.getConnection(url, props)
    try {
      io.unsafeRun(conn)
    } finally {
      conn.close()
    }
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
