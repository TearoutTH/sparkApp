package org.example.parserUtils;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RuleParser {
    private static JSONObject json;

    private static Rule rule = new Rule();

    {
        readJson();
    }

    public static Rule getRule() {
        return rule;
    }

    private static void readJson() {
        String content;
        try {
            content = new String(Files.readAllBytes(Paths.get("src/main/resources/rule.json")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        json = new JSONObject(content);
    }

    private static void parseJoins() {
        JSONArray joinsJson = json.getJSONArray("joins");
        List<Join> joins = new ArrayList<>();
        for (int i = 0; i < joinsJson.length(); i++) {
            JSONObject joinJson = joinsJson.getJSONObject(i);
            String tableLeft = joinJson.optString("table_left");
            String entityLeft = joinJson.optString("entity_left");
            String tableRight = joinJson.optString("table_right");
            String entityRight = joinJson.optString("entity_right");
            String type = joinJson.optString("type");
            joins.add(new Join(tableLeft, entityLeft, tableRight, entityRight, type));
        }
        rule.setJoins(joins);
    }

    private static void parseVariables() {
        JSONArray variablesJson = json.getJSONArray("variables");
        List<Variable> variables = new ArrayList<>();
        for (int i = 0; i < variablesJson.length(); i++) {
            JSONObject variableJson = variablesJson.getJSONObject(i);
            String name = variableJson.optString("name");
            String type = variableJson.optString("type");
            variables.add(new Variable(name, type));
        }
        rule.setVariables(variables);
    }

    private static void parseParameters() {
        JSONArray paramsJson = json.getJSONArray("parameters");
        List<Parameter> parameters = new ArrayList<>();
        for (int i = 0; i < paramsJson.length(); i++) {
            JSONObject paramJson = paramsJson.getJSONObject(i);
            String name = paramJson.optString("name");
            String value = paramJson.optString("value");
            String type = paramJson.optString("type");
            parameters.add(new Parameter(name, value, type));
        }
        rule.setParameters(parameters);
    }

    private static void parseCriterias() {
        JSONArray criteriasJson = json.getJSONArray("criterias");
        List<Criteria> criterias = new ArrayList<>();
        for (int i = 0; i < criteriasJson.length(); i++) {
            JSONObject criteriaJson = criteriasJson.getJSONObject(i);
            String id = criteriaJson.optString("id");
            String parameter = criteriaJson.optString("parameter");
            String operator = criteriaJson.optString("operator");
            String value = criteriaJson.optString("value");
            if (value.startsWith("{")) {
                criterias.add(new Criteria(id, parameter, operator, value.substring(1, value.length()-1)));
            } else {
                criterias.add(new Criteria(id, parameter, operator, value));
            }
        }
        rule.setCriterias(criterias);
    }

    private static void parseTree() {
        JSONArray treeJson = json.getJSONArray("tree");
        Map<String, Node> nodes = new LinkedHashMap<>();
        for (int i = 0; i < treeJson.length(); i++) {
            JSONObject nodeJson = treeJson.getJSONObject(i);
            Node n = parseNode(nodeJson);
            nodes.put(n.getId(), n);
        }
        for (Node n: nodes.values()) {
            if (n.getTrueChild() != null) {
                Node kid = n.getTrueChild();
                if (!kid.isLeaf() && kid.getCriteriasId() == null) {
                    n.setTrueChild(nodes.get(kid.getId().substring(1)));
                }
                kid = n.getFalseChild();
                if (!kid.isLeaf() && kid.getCriteriasId() == null) {
                    n.setFalseChild(nodes.get(kid.getId().substring(1)));
                }
            }
        }
        Node tree = nodes.values().stream().findFirst().orElseThrow();
        rule.setTree(tree);
    }

    private static Node parseNode(JSONObject nodeJson) {
        if (!nodeJson.optString("operator").equals("")) {
            String id = nodeJson.optString("id");
            List<String> criteriasId = new ArrayList<>();
            JSONArray criterias = nodeJson.getJSONArray("criterias");
            for (int j = 0; j < criterias.length(); j++) {
                criteriasId.add(criterias.optString(j));
            }
            String operator = nodeJson.optString("operator");
            JSONArray nodes = nodeJson.getJSONArray("nodes");
            return new Node(false,id,criteriasId,operator, parseNode(nodes.getJSONObject(0)), parseNode(nodes.getJSONObject(1)),0.0);
        } else if (!nodeJson.optString("condition").equals("")) {
            String id = nodeJson.optString("id");
            return new Node(false, "0" + id, null, null, null, null, 0.0);
        } else {
            String id = nodeJson.optString("id");
            double loss = nodeJson.getDouble("loss");
            return new Node(true, id,null, null, null, null, loss);
        }
    }

    public static void parseAll() {
        parseJoins();
        parseVariables();
        parseParameters();
        parseCriterias();
        parseTree();
    }
}
