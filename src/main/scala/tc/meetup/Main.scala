package tc.meetup

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.{Properties, UUID}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object Main {

  case class Meetup(id: UUID, title: String, summary: Option[String], attendees: Integer, pizzas: Option[Integer])

  case class NeedsSummary(id: UUID, meetupId: UUID)

  // A Program is any chunk of code that can be chained or mapped. Unlike StatementIO, which is
  // specific to JDBC statements. Any type that can prove it is a Program can be used.
  trait Program[F[_]] {
    def finish[A](a: => A): F[A]

    def chain[A, B](fa: F[A], afb: A => F[B]): F[B]

    def map[A, B](fa: F[A], ab: A => B): F[B]
  }
  object Program {
    def apply[F[_]](implicit F: Program[F]): Program[F] = F
  }
  // Implicit classes add functions to types that satisfy Program. This is provides for types F
  // which do not already have map and flatMap. StatementIO already has those but other types might not.
  implicit class ProgramSyntax[F[_], A](fa: F[A]) {
    def map[B](f: A => B)(implicit F: Program[F]): F[B] = F.map(fa, f)

    def flatMap[B](afb: A => F[B])(implicit F: Program[F]): F[B] = F.chain(fa, afb)
  }
  def finish[F[_], A](a: => A)(implicit F: Program[F]): F[A] = F.finish(a)

  // Just like Program but for different functionality. This is for any chunk of code that invokes JDBC.
  trait JdbcDatabase[F[_]] {
    def list[B](query: String, f: ResultSet => Try[List[B]]): F[Try[List[B]]]

    def update(query: String, f: PreparedStatement => Try[PreparedStatement]): F[Try[Int]]
  }
  object JdbcDatabase {
    def apply[F[_]](implicit F: JdbcDatabase[F]): JdbcDatabase[F] = F
  }
  implicit class JdbcDatabaseSyntax[F[_], A](fa: F[A]) {
    def list[B](query: String, f: ResultSet => Try[List[B]])(implicit F: JdbcDatabase[F]): F[Try[List[B]]] = F.list(query, f)

    def update(query: String, f: PreparedStatement => Try[PreparedStatement])(implicit F: JdbcDatabase[F]): F[Try[Int]] = F.update(query, f)
  }

  case class StatementIO[A](unsafeRun: Connection => A) { self =>

    def map[B](f: A => B): StatementIO[B] = StatementIO((conn: Connection) => f(self.unsafeRun(conn)))

    def flatMap[B](f: A => StatementIO[B]): StatementIO[B] = StatementIO((conn: Connection) => f(self.unsafeRun(conn)).unsafeRun(conn))

    def list[B](query: String, f: ResultSet => Try[List[B]]): StatementIO[Try[List[B]]] =
      StatementIO((conn: Connection) => listHelper(query, f)(conn))

    def update(query: String, f: PreparedStatement => Try[PreparedStatement]): StatementIO[Try[Int]] =
      StatementIO((conn: Connection) => updateHelper(query, f)(conn))
  }
  object StatementIO {
    def point[A](a: A): StatementIO[A] = StatementIO((conn: Connection) => a)

    // StatementIO needs to prove that it can satisfy Program and JdbcDatabase F types.
    // In Scala, this is done with implicits like below.
    implicit val ProgramIO = new Program[StatementIO] {
      override def finish[A](a: => A): StatementIO[A] = StatementIO.point(a)

      override def chain[A, B](fa: StatementIO[A], afb: A => StatementIO[B]): StatementIO[B] = fa.flatMap(afb)

      override def map[A, B](fa: StatementIO[A], ab: A => B): StatementIO[B] = fa.map(ab)
    }

    implicit val JdbcDatabaseIO = new JdbcDatabase[StatementIO] {
      override def list[B](query: String, f: ResultSet => Try[List[B]]): StatementIO[Try[List[B]]] = StatementIO(conn => listHelper(query, f)(conn))

      override def update(query: String, f: PreparedStatement => Try[PreparedStatement]): StatementIO[Try[Int]] = StatementIO(conn => updateHelper(query, f)(conn))
    }
  }

  // Another type with satisfies Program and JdbcDatabase but does nothing.
  case class Foo[A](a: A)
  object Foo {
    implicit val ProgramFoo = new Program[Foo] {
      override def finish[A](a: => A): Foo[A] = new Foo(a)

      override def chain[A, B](fa: Foo[A], afb: A => Foo[B]): Foo[B] = afb(fa.a)

      override def map[A, B](fa: Foo[A], ab: A => B): Foo[B] = Foo(ab(fa.a))
    }

    implicit val JdbcDatabaseFoo = new JdbcDatabase[Foo] {
      override def list[B](query: String, f: ResultSet => Try[List[B]]): Foo[Try[List[B]]] = Foo(Success(List()))

      override def update(query: String, f: PreparedStatement => Try[PreparedStatement]): Foo[Try[Int]] = Foo(Success(42))
    }
  }


  def main(args: Array[String]): Unit = {
    System.out.println("Starting meetup checker")

    val io: StatementIO[Try[Int]] = checkMeetups[StatementIO]()
    val result = unsafeExecute(io)

    System.out.println(s"""Result: $result""")

    val foo = checkMeetups[Foo]()
    System.out.println("Foo: " + foo.a)

    System.out.println("Ending meetup checker")
  }

  // We have killed our reliance on StatementIO. We can now run this program with any type that
  // satisfies Program and JdbcDatabase and we can chain with other Programs.
  def checkMeetups[F[_]: Program: JdbcDatabase](): F[Try[Int]] = {
    finish(())
      .list(Sql.SelectAll, fromResultSet)
      .flatMap {
        case Failure(error) => finish(Failure(error))
        case Success(meetups: List[Meetup]) =>
          meetups.map(meetup => {
            (meetup.summary, meetup.pizzas) match {
              case (None, _) =>
               finish(()).update(Sql.CreateNeedsSummary, setSummaryNotification(UUID.randomUUID(), meetup.id))
              case (_, None) =>
                finish(()).update(Sql.UpdatePizzas, setPizzaCount(meetup.id, meetup.attendees / 3))
              case _ =>
                finish[F, Try[Int]](Success(0))
            }
          }).fold(finish[F, Try[Int]](Success(0))) { (m1, m2) =>
            m1.flatMap(e1 => m2.map(e2 => e1.flatMap(r1 => e2.map(r2 => r1 + r2))))
          }
      }
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
