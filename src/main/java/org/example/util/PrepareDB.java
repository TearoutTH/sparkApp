package org.example.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class PrepareDB {

    public static void execute(SparkSession spark) {

        //Create database
        String dbName = "tmp_tos";
        spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s", dbName));

        // Create tables
        try {
            spark.sql(String.join("\n", Files.readAllLines(Paths.get("/home/" + System.getenv("USER") + "/container/vsa_nd_nds_r1.sql"))));
            spark.sql(String.join("\n", Files.readAllLines(Paths.get("/home/" + System.getenv("USER") + "/container/vsa_nd_nds_r3.sql"))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //Load data in table tmp_tos.vsa_nd_nds_r1
        if (spark.table("tmp_tos.vsa_nd_nds_r1").count() == 0) {

            String tableName = "tmp_tos.vsa_nd_nds_r1";
            String inputPath = "/home/" + System.getenv("USER") + "/container/tmp_tos_vsa_nd_nds_r1_2.tsv";

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
            String inputPath = "/home/" + System.getenv("USER") + "/container/tmp_tos_vsa_nd_nds_r3_2.tsv";

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
    }
}
