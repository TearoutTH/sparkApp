package org.example.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class PrepareDB {
    private static Properties props = new Properties();

    public static void execute(SparkSession spark) {
        // Create database
//        spark.sql(String.format("DROP DATABASE IF EXISTS tmp_tos CASCADE"));
//        spark.sql(String.format("DROP TABLE tmp_tos.vsa_nd_nds_r1"));
//        spark.sql(String.format("REFRESH tmp_tos.vsa_nd_nds_r3"));

        String dbName = "tmp_tos";
        spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s", dbName));

        // Create tables
        try {
            spark.sql(String.join("\n", Files.readAllLines(Paths.get("src/main/resources/vsa_nd_nds_r1.sql"))));
            spark.sql(String.join("\n", Files.readAllLines(Paths.get("src/main/resources/vsa_nd_nds_r3.sql"))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //Load data in table tmp_tos.vsa_nd_nds_r1
        if (spark.table("tmp_tos.vsa_nd_nds_r1").count() == 0) {

            String tableName = "tmp_tos.vsa_nd_nds_r1";
            String inputPath = "src/main/resources/tmp_tos_vsa_nd_nds_r1_2.tsv";

            Dataset<Row> ds = spark.read()
                    .format("csv")
                    .option("delimiter", "\t")
                    .option("header", "true")
                    .option("inferSchema", true)
                    .load(inputPath);

            ds.write()
                    .format("orc")
                    .insertInto(tableName);
        }
        //Load data in table tmp_tos.vsa_nd_nds_r3
        if (spark.table("tmp_tos.vsa_nd_nds_r3").count() == 0) {

            String tableName = "tmp_tos.vsa_nd_nds_r3";
            String inputPath = "src/main/resources/tmp_tos_vsa_nd_nds_r3_2.tsv";

            Dataset<Row> ds = spark.read()
                    .format("csv")
                    .option("delimiter", "\t")
                    .option("header", "true")
                    .option("inferSchema", true)
                    .load(inputPath);

            ds.write()
                    .format("orc")
                    .insertInto(tableName);
        }
        spark.table("tmp_tos.vsa_nd_nds_r1").show(10);
        spark.table("tmp_tos.vsa_nd_nds_r3").show(10);
    }
}
