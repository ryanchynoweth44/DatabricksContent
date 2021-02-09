
import java.sql.DriverManager
import java.sql.Connection 

object ScalaSqlAnalyticsConnect {

    def main(args: Array[String]) : Unit = {
        val driver = "com.simba.spark.jdbc.Driver"
        val url = "<JDBC URL to Spark Cluster or SQL Endpoint>" 
        val username = "token"
        val password = "<PAT TOKEN>"
        val query = "<SPARK SQL QUERY>"

        // make the connection
        Class.forName(driver)
        var connection = DriverManager.getConnection(url, username, password)

        // create the statement, and run the select query
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery(query)
        while ( resultSet.next() ) {
            println(resultSet.getString("<ColName1>"))
            println(resultSet.getString("<ColName2>"))

            
        }

            connection.close()
    }

}