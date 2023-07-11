package readSpark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String csv[]){
        SparkSession sparkSession= SparkSession.builder().master("local").appName("Read_CSV").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().option("header","true").csv("C:/Users/Ulniuc Maria/Desktop/javaPractice/learnSpark/src/main/resources/erasmus.csv");


        dataset.show(15, false);
    }
}
