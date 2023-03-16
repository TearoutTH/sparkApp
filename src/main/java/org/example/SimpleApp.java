package org.example;
import org.apache.spark.sql.SparkSession;
import org.example.parserUtils.Node;
import org.example.util.Evaluate;
import org.example.view.ViewCreator;
import org.example.parserUtils.RuleParser;
import org.example.parserUtils.Join;
import org.example.util.PrepareDB;

import javax.swing.text.View;
import java.util.List;

public class SimpleApp {
    private static final RuleParser RULE_PARSER = new RuleParser();
    public static void main(String[] args) {
        String warehouseLocation = "/home/" + System.getenv("USER") + "/hive/warehouse";
        SparkSession spark = SparkSession.builder()
                .master("local")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .appName("Simple Application")
                .config("spark.sql.catalogImplementation", "hive")
                .getOrCreate();
        PrepareDB.execute(spark);
        RuleParser.parseAll();
        ViewCreator.createViewAfterJoins(RuleParser.getRule().getJoins(), spark);
        Evaluate.evaluate(spark);
        spark.stop();
    }
}