package org.example;
import org.apache.spark.sql.SparkSession;
import org.example.view.ViewCreator;
import org.example.parserUtils.RulesParser;
import org.example.parserUtils.Join;
import org.example.util.PrepareDB;

import java.util.ArrayList;
import java.util.List;

public class SimpleApp {
    private static final RulesParser rulesParser = new RulesParser("src/main/resources/rule.json", new ArrayList<>(), variables);
    public static void main(String[] args) {
        String logFile = "src/main/resources/text.txt";
        String warehouseLocation = "/home/" + System.getenv("USER") + "/hive/warehouse";
        SparkSession spark = SparkSession.builder()
                .master("local")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .appName("Simple Application")
                .config("spark.sql.catalogImplementation", "hive")
                .getOrCreate();
        PrepareDB.execute(spark);

        rulesParser.parseJoins();
        List<Join> joins = rulesParser.getJoins();
        ViewCreator.createViewAfterJoins(joins, spark);

        spark.stop();
    }
}