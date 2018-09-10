package SQL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}

object TravelSQL {
  case class HolidaysDetails(UID: Int, Arrival: String, Destination: String, Transport_Mode: String, Distance: Int, Year: Long)

  case class TransportMode(Trans_Mode: String, Trans_Expense: Int)

  case class UserID(userID: Int, userName: String, userAge: Int)

  def main (args: Array[String]): Unit = {

    println("hello Scala, this is Travel Data Analysis using Spark SQL")

    /***Created a spark session object for spark application***/
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL Travel Example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //Set the log level as warning
    spark.sparkContext.setLogLevel("WARN")

    println("Spark Session Object Created")

    /***For implicit conversions like converting RDDs and sequences to DataFrames***/
    import spark.implicits._

    /***Loading Holidays Dataset***/
    val HolidaysFromFile = spark.sparkContext
      .textFile("D:\\AcadGild\\ScalaCaseStudies\\Datasets\\Travel\\s20\\Holidays.txt")
      .map(_.split(","))
      .map(x => HolidaysDetails(x(0).toInt, x(1), x(2), x(3), x(4).toInt, x(5).toInt))
      .toDF()
    HolidaysFromFile.show()
    print("Holidays dataframe displayed\n\n\n")

    /***Loading Transport Dataset***/
    val TransportFromFile = spark.sparkContext
      .textFile("D:\\AcadGild\\ScalaCaseStudies\\Datasets\\Travel\\s20\\TransportMode.txt")
      .map(_.split(","))
      .map(x => TransportMode(x(0), x(1).toInt))
      .toDF()
    TransportFromFile.show()
    print("Transport mode dataframe displayed\n\n\n")

    /***Loading User Details Dataset***/
    val UserIDFromFile = spark.sparkContext
      .textFile("D:\\AcadGild\\ScalaCaseStudies\\Datasets\\Travel\\s20\\UserDetails.txt")
      .map(_.split(","))
      .map(x => UserID(x(0).toInt, x(1), x(2).toInt))
      .toDF()
    UserIDFromFile.show()
    print("User Details dataframe displayed\n\n\n")

    /***What is the distribution of the total number of air-travelers per year?***/
    /***Create a view on the Data Frames called "HolidayData"***/
    print("HolidayData View created\n")
    print("Distribution of total number of air-travelers per year analyzed\n")
    HolidaysFromFile.createOrReplaceTempView("HolidayData")
    spark.sql(""" select Year, count("Year") from HolidayData Group by Year""")
      .show()

    /***What is the total air distance covered by each user per year?***/
    print("Users total air distance per year analyzed\n")
    val joindf = HolidaysFromFile.as ('a)
      .join(UserIDFromFile.toDF().as('b), $"a.UID" === $"b.userID")
    joindf.createOrReplaceTempView("joinView")
    spark.sql(""" select UID,userName,Year,Sum(Distance)as TotalDistance from joinView
        |Group by Year,UID,userName
        |order by TotalDistance desc""".stripMargin)
      .show()

    /***Which user has travelled the largest distance till date?***/
    print("Users with largest distance analyzed\n")
    val task1 = spark.sql("""select UID,userName,Year,Sum(Distance)as TotalDistance from joinView
        |Group by Year,UID,userName""".stripMargin)
    task1.toDF().createOrReplaceTempView("DistanceView")

    spark.sql(""" select UID,userName,Year,Max(TotalDistance) as MaximumDistance from DistanceView
        |Group by Year,UID,userName
        |order by MaximumDistance desc """.stripMargin)
      .show(1)

    /***What is the most preferred destination for all users?***/
    print("Most preferred destination analyzed\n")
    spark.sql("""select Destination,count(Destination) as mostPrefDest
        |from joinView group by Destination
        |order by mostPrefDest desc """.stripMargin)
      .show(1)

    /***Which route is generating the most revenue per year?***/
    print("revenueView Created\n")
    val joineddf = HolidaysFromFile.as('c)
      .join(TransportFromFile.toDF().as('d),$"c.Transport_Mode" === $"d.Trans_Mode", joinType = "left_outer")
    joineddf.createOrReplaceTempView("revenueView")


    print("maxRevenueView Created\n")
    val revenue = spark.sql("""select UID,Destination,Transport_Mode,Year,count(Transport_Mode) * sum(Trans_Expense)
        |as revenueExpense from revenueView
        |group by UID,Year,Destination,Transport_Mode""".stripMargin)
    revenue.toDF().createOrReplaceTempView("maxRevenue")

    print("Route generating most revenue per year analyzed\n")
    spark.sql("""select Destination, Transport_Mode,Year,max(revenueExpense) as maximumRevenue from maxRevenue
        |group by Destination,Transport_Mode,Year
        |order by maximumRevenue desc""".stripMargin)
      .show()


    /***What is the total amount spent by every user on air-travel per year?***/
    val expense = spark.sql(""" select UID,Destination,Transport_Mode,Year,sum(Trans_Expense) as totalExpense from revenueView
        |group by UID,Year,Destination,Transport_Mode""".stripMargin)
      .filter(col("Transport_Mode") === "airplane")
    println("Transportation Expense calculated and filtered for Air Travel")

    val newJoindf = UserIDFromFile.toDF().as('e).join(expense.as('f), $"e.userID" === $"f.UID")

    newJoindf.toDF().createOrReplaceTempView("expenseView")
    println("ExpenseView Created\n")
    print("Total amount spent by every user on Air Travel per year analyzed\n")
    spark.sql(""" select UID, Transport_Mode, Year, totalExpense from expenseView
        |group by UID,Year,totalExpense,Transport_Mode""".stripMargin)
      .show()

    /***Considering age groups of < 20, 20-35, 35 >, which age group is travelling the most every year?***/
    print("Age wise grouping of travel data every year analyzed\n")
    spark.sql("""select userAge,count(UID) as countTravel from joinView WHERE userAge >= 20
        |AND userAge <= 35 group by userAge,UID
        |order by countTravel desc """.stripMargin)
      .show()
  }
}
