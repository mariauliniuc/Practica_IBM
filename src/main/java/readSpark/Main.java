package readSpark;

import org.apache.spark.sql.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import static org.apache.spark.sql.functions.col;

public class Main {
    public static void main(String csv[]) throws SQLException {
        //prima tema
        SparkSession sparkSession= SparkSession.builder().master("local").appName("Read_CSV").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().option("header","true").csv("C:/Users/Ulniuc Maria/Desktop/javaPractice/learnSpark/src/main/resources/erasmus.csv");
        //dataset.show(25, false);
        //a doua tema
        dataset.printSchema();
        dataset=dataset.filter(functions.col("Receiving Country Code").isin("ES", "FR","EL"));
        dataset.select("Receiving Country Code","Sending Country Code").show(50,false);
        dataset.groupBy("Receiving Country Code","Sending Country Code")
                .count().withColumnRenamed("count","Number of students")
                .orderBy(functions.col("Receiving Country Code").desc())
                .show(100);
        saveData(dataset,"ES","Estonia");
        saveData(dataset,"FR","Franta");
        saveData(dataset,"EL","Elvetia");
    }
    //a treia tema
    public static void saveData(Dataset<Row> dataset, String countryCode, String tableName) {
        dataset
                .filter(col("Receiving Country Code").isin(countryCode))
                .groupBy("Receiving Country Code", "Sending Country Code")
                .count().orderBy("Receiving Country Code", "Sending Country Code")
                .write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/erasmus?serverTimezone=UTC")
                .option("dbtable", tableName)
                .option("user", "root")
                .option("password", "root")
                .save(tableName + ".erasmus");
    }
}
