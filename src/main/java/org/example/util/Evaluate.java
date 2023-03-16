package org.example.util;

import org.apache.spark.sql.*;
import org.example.parserUtils.*;
import org.json.JSONArray;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Evaluate {

    private static Rule rule;
    public static void evaluate(SparkSession spark) {
        RuleParser.parseAll();
        rule = RuleParser.getRule();
        Node node = rule.getTree();
        Dataset<Row> result = evaluateNode(node, spark.table("my_view"));
        result = result.filter("loss > " + 0).select("fid", "year", "quarter");
        result.show(50);
        String jsonString = result.toJSON().collectAsList().toString();
        JSONArray jsonArray = new JSONArray(jsonString);
        try (FileWriter fileWriter = new FileWriter("src/main/resources/result.json")) {
            fileWriter.write(jsonArray.toString());
        } catch (IOException e) {
            System.err.println("Ошибка записи в файл: " + e.getMessage());
        }
    }

    private static Dataset<Row> evaluateNode(Node node, Dataset<Row> view) {
        Dataset<Row> beginView = view;
        List<String> criteriasId = node.getCriteriasId();
        String operator = node.getOperator();
        view.show(10);
        List<Parameter> parameters = rule.getParameters();
        if (operator != null && operator.equals("AND")) {
            for (String id :
                    criteriasId) {
                Criteria criteria = rule.getCriterias().stream().filter(c -> c.getId().equals(id)).findFirst().orElseThrow();
                String value;
                String nameofColumn;
                if (parameters.stream().anyMatch(parameter -> parameter.getName().equals(criteria.getParameter()))) {
                    Parameter param = parameters.stream().filter(parameter -> parameter.getName().equals(criteria.getParameter())).findFirst().get();
                    value = param.getValue();
                    Column resultCol = functions.expr(value).as("result");
                    view = view.select("*").withColumn("result", resultCol);
                    nameofColumn = "result";
                    view.show(15);
                } else {
                    nameofColumn = criteria.getParameter();
                }
                switch (criteria.getOperator()) {
                    case "lt": {
                        view = view.filter(nameofColumn + "<" + criteria.getValue());
                        break;
                    }
                    case "gt": {
                        view = view.filter(nameofColumn + ">" + criteria.getValue());
                        break;
                    }
                    case "eq": {
                        view = view.filter(nameofColumn + "=" + criteria.getValue());
                        break;
                    }
                }
                view = view.drop("result");
                view.show(10);
            }
        } else if (operator != null && operator.equals("OR")) {
            List<Dataset<Row>> listOfResults = new ArrayList<>();
            for (String id :
                    criteriasId) {
                Criteria criteria = rule.getCriterias().stream().filter(c -> c.getId().equals(id)).findFirst().orElseThrow();
                String value;
                String nameofColumn;
                if (parameters.stream().anyMatch(parameter -> parameter.getName().equals(criteria.getParameter()))) {
                    Parameter param = parameters.stream().filter(parameter -> parameter.getName().equals(criteria.getParameter())).findFirst().get();
                    value = param.getValue();
                    Column resultCol = functions.expr(value).as("result");
                    view = view.select("*").withColumn("result", resultCol);
                    nameofColumn = "result";
                    view.show(15);
                } else {
                    nameofColumn = criteria.getParameter();
                }
                switch (criteria.getOperator()) {
                    case "lt": {
                        view = view.filter(nameofColumn + "<" + criteria.getValue());
                        view = view.drop("result");
                        listOfResults.add(view);
                        break;
                    }
                    case "gt": {
                        view = view.filter(nameofColumn + ">" + criteria.getValue());
                        view = view.drop("result");
                        listOfResults.add(view);
                        break;
                    }
                    case "eq": {
                        view = view.filter(nameofColumn + "=" + criteria.getValue());
                        view = view.drop("result");
                        listOfResults.add(view);
                        break;
                    }
                }
            }
            Dataset<Row> trueRes = listOfResults.get(0);
            listOfResults.subList(1, listOfResults.size()).forEach(trueRes::union);
            view = trueRes.distinct();
            view.show(10);
        } else {
            view = view.withColumn("loss", functions.lit(node.getLoss()));
            return view;
        }
        Dataset<Row> trueResult = evaluateNode(node.getTrueChild(),view);
        Dataset<Row> falseCriteria = beginView.except(view);
        Dataset<Row> falseResult = evaluateNode(node.getFalseChild(), falseCriteria);
        return trueResult.union(falseResult).distinct();
    }
}
