package org.example.view;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.parserUtils.Join;

import java.util.List;

public class ViewCreator {
    public static void createViewAfterJoins(List<Join> joins, SparkSession spark) {

        Dataset<Row> leftTable = spark.table(joins.get(0).getTableLeft());
        Dataset<Row> rightTable = spark.table(joins.get(0).getTableRight());
        Dataset<Row> joinedTable = leftTable.join(rightTable,
                leftTable.col(joins.get(0).getEntityLeft()).equalTo(rightTable.col(joins.get(0).getEntityRight()))
                        .and(leftTable.col(joins.get(1).getEntityLeft()).equalTo(rightTable.col(joins.get(1).getEntityRight())))
                        .and(leftTable.col(joins.get(2).getEntityLeft()).equalTo(rightTable.col(joins.get(2).getEntityRight()))),
                joins.get(0).getType());
        joinedTable.createOrReplaceTempView("my_view");
    }
}
