package tc.meetup

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.{Properties, UUID}

object Main {

  case class Meetup(id: UUID, title: String, summary: Option[String], attendees: Integer, pizzas: Option[Integer])

  case class NeedsSummary(id: UUID, meetupId: UUID)

  def main(args: Array[String]): Unit = {
    System.out.println("Starting meetup checker")

    // This program is pretty simple. It queries meetups from the database and for each meetup,
    // either send a notification that we need a summary, or if the summary is set then update the
    // pizza count if needed. For simplicity, this doesn't do transactions or batching.
    var conn: Connection = null
    try {
      conn = getConnection
      var selectAllStatement: PreparedStatement = null
      var selectAllResults: ResultSet = null
      var meetups: List[Meetup] = List()

      try {
        selectAllStatement = conn.prepareStatement(Sql.SelectAll)
        selectAllResults = selectAllStatement.executeQuery()

        while (selectAllResults.next()) {
          val meetup = Meetup(
            selectAllResults.getObject(Columns.Id, classOf[UUID]),
            selectAllResults.getString(Columns.Title),
            Option(selectAllResults.getString(Columns.Summary)),
            selectAllResults.getInt(Columns.Attendees),
            Option(selectAllResults.getObject(Columns.Pizzas, classOf[Integer]))
          )
          meetups = meetups :+ meetup
        }
      } finally {
        if (selectAllResults != null) selectAllResults.close()
        if (selectAllStatement != null) selectAllStatement.close()
      }

      val result = meetups
        .map(meetup => {
          if (meetup.summary.isDefined) {
            if (meetup.pizzas.isEmpty) {
              var updateStatement: PreparedStatement = null
              try {
                updateStatement = conn.prepareStatement(Sql.UpdatePizzas)
                updateStatement.setInt(1, meetup.attendees / 3)
                updateStatement.setObject(2, meetup.id)
                updateStatement.executeUpdate()
              } finally {
                if (updateStatement != null) updateStatement.close()
              }
            } else {
              0
            }
          } else {
            var insertStatement: PreparedStatement = null
            try {
              insertStatement = conn.prepareStatement(Sql.CreateNeedsSummary)
              insertStatement.setObject(1, UUID.randomUUID())
              insertStatement.setObject(2, meetup.id)
              insertStatement.executeUpdate()
            } finally {
              if (insertStatement != null) insertStatement.close()
            }
          }
        })
        .sum

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
