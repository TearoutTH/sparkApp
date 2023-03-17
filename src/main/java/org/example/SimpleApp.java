package org.example;

import org.apache.spark.sql.SparkSession;
import org.example.parserUtils.RuleParser;
import org.example.util.Evaluate;
import org.example.util.PrepareDB;
import org.example.view.ViewCreator;
import org.json.JSONArray;

import java.io.FileWriter;
import java.io.IOException;

public class SimpleApp {
    public static void main(String[] args) {
        String warehouseLocation = "/home/" + System.getenv("USER") + "/hive/warehouse";
        //Build spark session
        SparkSession spark = SparkSession.builder()
                .master("local")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .appName("Simple Application")
                .config("spark.sql.catalogImplementation", "hive")
                .getOrCreate();
        //Preparing DB
        PrepareDB.execute(spark);
        //Parsing JSON
        RuleParser.parseAll();
        //Creating view
        ViewCreator.createViewAfterJoins(RuleParser.getRule().getJoins(), spark);
        //Evaluating answer
        JSONArray array = Evaluate.evaluate(spark);
        // Write out result in file
        try (FileWriter fileWriter = new FileWriter("/home/" + System.getenv("USER") + "/container/result.json")) {
            fileWriter.write(array.toString());
        } catch (IOException e) {
            System.err.println("Error was detected while writing file result.json: " + e.getMessage());
        }
        spark.stop();
    }
}