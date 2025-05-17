import scala.io.Source
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone, Days, LocalDate}
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.io.{File, FileWriter, PrintWriter}

object main extends App {
  // Logger
  object Logger {
    private val logFile = new File("rules_engine.log")

    def log(level: String, message: String): Unit = {
      val timestamp = DateTime.now().toString("yyyy-MM-dd'T'HH:mm:ssZ")
      val logEntry = s"$timestamp:[$level]:$message\n"

      val writer = new PrintWriter(new FileWriter(logFile, true))
      try {
        writer.write(logEntry)
      } finally {
        writer.close()
      }
    }

    def info(message: String): Unit = log("INFO", message)
    def warn(message: String): Unit = log("WARN", message)
    def error(message: String): Unit = log("ERROR", message)
  }

  // Database writer (singleton)
  object DatabaseWriter {
    // Configuration
    private val url = "jdbc:postgresql://localhost:5444/orders"
    private val user = "user"
    private val password = "123"

    // Creates SQL statement
    private def createStatement(connection: Connection): PreparedStatement = {
      connection.prepareStatement(
        """
      INSERT INTO product_discounts
      (transaction_date, product_name, expiry_date, quantity, unit_price, channel, payment_method, discount, final_price)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      """
      )
    }

    // binds parameters
    private def bindParameters(statement: PreparedStatement, productWithDiscount: ProductWithDiscount): PreparedStatement = {
      val p = productWithDiscount.product
      statement.setDate(1, new java.sql.Date(p.transaction_date.toDateTimeAtStartOfDay.getMillis))
      statement.setString(2, p.product_name)
      statement.setDate(3, new java.sql.Date(p.expiry_date.toDateTimeAtStartOfDay.getMillis))
      statement.setInt(4, p.quantity)
      statement.setDouble(5, p.unit_price)
      statement.setString(6, p.channel)
      statement.setString(7, p.payment_method)
      statement.setDouble(8, productWithDiscount.discount)
      statement.setDouble(9, productWithDiscount.final_price)
      statement
    }

    // Wrapper to handle database connection and write data
    def writeProductWithDiscount(productWithDiscount: ProductWithDiscount): Unit = {
          var connection: Connection = null
          try {
            Class.forName("org.postgresql.Driver")
            connection = DriverManager.getConnection(url, user, password)

            val statement = createStatement(connection)
            val boundStatement = bindParameters(statement, productWithDiscount)

            boundStatement.executeUpdate()
            Logger.info(s"Successfully wrote to database: ${productWithDiscount.product.product_name}")
          } catch {
            case e: Exception =>
              Logger.error(s"Failed to write to database: ${e.getMessage}")
              throw e
          } finally {
            if (connection != null) connection.close()
          }
      }
    }


  val lines: List[String] = Source.fromFile("src/resources/TRX1000.csv").getLines().toList.tail

  case class Product (transaction_date: LocalDate,product_name: String,expiry_date: LocalDate,quantity: Int,unit_price: Double
                      ,channel: String,payment_method: String)
  case class ProductWithDiscount(product: Product, discount: Double, final_price: Double)

  def mapLinesToProducts(line: String): Product = {
    // maps csv line to product object
    val line_parts = line.split(",")
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC)
    val transaction_date = formatter.parseDateTime(line_parts(0).split("T")(0)).toLocalDate
    val expiry_date = formatter.parseDateTime(line_parts(2)).toLocalDate

    Product(
      transaction_date, line_parts(1), expiry_date, line_parts(3).toInt,
      line_parts(4).toDouble, line_parts(5), line_parts(6)
    )
  }
  // returns true if there are less than 30 days till expiry from transaction date
  def isSoldWithinLast30(product: Product): Boolean = Days.daysBetween(product.transaction_date, product.expiry_date).getDays < 30

  // returns true if the product is wine or cheese
  def isWineOrCheese(product: Product): Boolean = product.product_name.startsWith("Wine") | product.product_name.startsWith("Cheese")

  // returns true if the product was sold on the 23rd of March
  def isSoldOn23March(product: Product): Boolean = product.transaction_date.getMonthOfYear == 3 & product.transaction_date.getDayOfMonth == 23

  // returns true if the product quantity sold is more than 5
  def isQuantityMoreThan5(product: Product): Boolean = product.quantity > 5

  // returns true if the product sold through app
  def isSoldByApp(product: Product): Boolean = product.channel.equals("App")

  // returns true if the payment method is Visa
  def isVisa(product: Product): Boolean = product.payment_method.equals("Visa")

  // Calculation rules
  def calcSoldWithinLast30Discount(product: Product): Double = (30 - Days.daysBetween(product.transaction_date, product.expiry_date).getDays) / 100.0

  def calcWineOrCheeseDiscount(product: Product) : Double = if(product.product_name.startsWith("Cheese")) 0.10 else 0.05

  def calcSoldOn23MarchDiscount(product: Product): Double = 0.50

  def calcQuantityMoreThan5Discount(product: Product): Double = if(product.quantity > 5 & product.quantity < 10) 0.05
                    else if(product.quantity >= 10 & product.quantity < 15) 0.07
                    else 0.10

  def calcSoldByAppDiscount(product: Product): Double = math.ceil(product.quantity / 5.0) * 5.0 / 100.0

  def calcVisaDiscount (product: Product): Double =  0.05

  // Rule engine

  // returns a list of tuples each tuple holds the qualification rule and the corresponding calculation rule
  def getDiscountRules: List[(Product => Boolean, Product => Double)] = List((isSoldWithinLast30, calcSoldWithinLast30Discount),
    (isWineOrCheese, calcWineOrCheeseDiscount), (isSoldOn23March, calcSoldOn23MarchDiscount),
    (isQuantityMoreThan5, calcQuantityMoreThan5Discount), (isSoldByApp, calcSoldByAppDiscount),
    (isVisa, calcVisaDiscount))

  // apply discount rules on each order and returns qualified discounts for each order
  def getQualifiedDiscountsWithLogging(product: Product, discountRules: List[(Product => Boolean, Product => Double)]): (Product,List[Double])= {
    Logger.info(s"Processing order: ${product.product_name}")
    def getQualifiedDiscounts(product: Product, discountRules: List[(Product => Boolean, Product => Double)]): (Product, List[Double]) = {
      val applicableDiscounts = discountRules.collect {
        case (qualifies, calculate) if qualifies(product) => calculate(product)
      }
      (product, applicableDiscounts)
    }
    val orderWithValidDiscounts = getQualifiedDiscounts(product, discountRules)
    Logger.info(s"Order: ${product.product_name} has qualified discounts: ${orderWithValidDiscounts._2}")
    orderWithValidDiscounts
  }
  // returns the final discount for each order
  def getOrderDiscountWithLogging(product: Product, applicableDiscounts: List[Double]): ProductWithDiscount = {
    Logger.info(s"Calculating discounts for order: ${product.product_name}")
    def getOrderDiscount(product: Product, applicableDiscounts: List[Double]): ProductWithDiscount = {
      val discount = if (applicableDiscounts.size >= 2) applicableDiscounts.sortBy(-_).take(2).sum / 2
                    else if (applicableDiscounts.size == 1) applicableDiscounts.head
                    else 0.0
      val final_price = product.unit_price * product.quantity * discount
      ProductWithDiscount(product, discount, final_price )
    }
    val orderWithFinalDiscount = getOrderDiscount(product, applicableDiscounts)
    if (orderWithFinalDiscount.discount > 1) Logger.warn(s"Order: ${orderWithFinalDiscount.product.product_name} has a final discount of: ${orderWithFinalDiscount.discount} which is greater than 100%")
    Logger.info(s"Order: ${orderWithFinalDiscount.product.product_name} has a final discount of: ${orderWithFinalDiscount.discount} and a final price of ${orderWithFinalDiscount.final_price} ")
    orderWithFinalDiscount
  }

  // running it all
    lines.map(mapLinesToProducts)
      .map(p => getQualifiedDiscountsWithLogging(p, getDiscountRules))
      .map(t => getOrderDiscountWithLogging(t._1,t._2))
      .foreach{ productWithDiscount =>
        println(productWithDiscount)
        DatabaseWriter.writeProductWithDiscount(productWithDiscount)
      }


}