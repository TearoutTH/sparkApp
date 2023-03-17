package org.example.util;

import org.apache.spark.sql.*;
import org.example.parserUtils.*;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

public class Evaluate {

    private static Rule rule;
    public static JSONArray evaluate(SparkSession spark) {
        RuleParser.parseAll();
        rule = RuleParser.getRule();
        Node node = rule.getTree();
        Dataset<Row> result = evaluateNode(node, spark.table("my_view"));
        result = result.filter("loss > " + 0).select("fid", "year", "quarter");
        String jsonString = result.toJSON().collectAsList().toString();
        return new JSONArray(jsonString);
    }

    private static Dataset<Row> evaluateNode(Node node, Dataset<Row> view) {
        Dataset<Row> beginView = view;
        List<String> criteriasId = node.getCriteriasId();
        String operator = node.getOperator();
        List<Parameter> parameters = rule.getParameters();
        if (operator != null && operator.equals("AND")) {
            //check all criteria
            for (String id :
                    criteriasId) {
                Criteria criteria = rule.getCriterias().stream().filter(c -> c.getId().equals(id)).findFirst().orElseThrow();
                String value;
                String nameofColumn;
                //if it has parameter
                if (parameters.stream().anyMatch(parameter -> parameter.getName().equals(criteria.getParameter()))) {
                    Parameter param = parameters.stream().filter(parameter -> parameter.getName().equals(criteria.getParameter())).findFirst().get();
                    value = param.getValue();
                    Column resultCol = functions.expr(value).as("result");
                    view = view.select("*").withColumn("result", resultCol);
                    nameofColumn = "result";
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
            }
            // same but with "OR"
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
            // It's "OR", so we combine all DataSets
            Dataset<Row> trueRes = listOfResults.get(0);
            listOfResults.subList(1, listOfResults.size()).forEach(trueRes::union);
            view = trueRes.distinct();
        } else {
            view = view.withColumn("loss", functions.lit(node.getLoss()));
            return view;
        }
        //Using recursion, same method, but with TrueChild and FalseChild
        Dataset<Row> trueResult = evaluateNode(node.getTrueChild(),view);
        Dataset<Row> falseCriteria = beginView.except(view);
        Dataset<Row> falseResult = evaluateNode(node.getFalseChild(), falseCriteria);
        return trueResult.union(falseResult).distinct();
    }
}
