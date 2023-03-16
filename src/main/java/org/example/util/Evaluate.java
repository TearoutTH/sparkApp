package org.example.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.parserUtils.*;
import org.stringtemplate.v4.ST;
import scala.Function1;

import java.math.BigDecimal;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class Evaluate {

    private static Rule rule;
    public static void evaluate(SparkSession spark) {
        RuleParser.parseAll();
        rule = RuleParser.getRule();
        Node node = rule.getTree();
        evaluateNode(node, spark);
    }

    private static void evaluateNode(Node node, SparkSession spark) {
        List<String> criteriasId = node.getCriteriasId();
        String operator = node.getOperator();
        Dataset<Row> view = spark.table("my_view");
        view.show(10);
        List<Parameter> parameters = rule.getParameters();
        if (operator.equals("AND")) {
            boolean result = true;
            for (String id:
                 criteriasId) {
                Criteria criteria = rule.getCriterias().stream().filter(c -> c.getId().equals(id)).findFirst().orElseThrow();
                String value;
                Dataset<Row> res;
                if (parameters.stream().anyMatch(parameter -> parameter.getName().equals(criteria.getParameter()))) {
                    Parameter param = parameters.stream().filter(parameter -> parameter.getName().equals(criteria.getParameter())).findFirst().get();
                    value = param.getValue();
                    res = view.selectExpr(value + " as result");
                    res.show();
                } else {
                    value = null;
                    res = null;
                }
                switch (criteria.getOperator()) {
                    case "lt": {
                        view = view.withColumn("result",res.col("result"));
                        view = view.filter("res" + "<" + criteria.getValue());
                        break;
                    }
                    case "gt": {
                        view = view.withColumn("res", res.col("result")).filter("res" + ">" + criteria.getValue());
                        break;
                    }
                    case "eq": {
                        view = view.withColumn("res", res.col("result")).filter("res" + "=" + criteria.getValue());
                        break;
                    }
                }
                view.show(10);
            }
        }
    }
}
