package org.example.parserUtils;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class RulesParser {
    private final String filePath;
    private final List<Join> joins;

    private final List<Variable> variables;

    public RulesParser(String filePath) {
        this.filePath = filePath;
        this.joins = new ArrayList<>();
        this.variables = new ArrayList<>();
    }

    public List<Join> getJoins() {
        return joins;
    }

    private JSONObject readJson() {
        String content;
        try {
            content = new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new JSONObject(content);
    }

    public void parseJoins() {
        JSONObject json = readJson();
        JSONArray joinsJson = json.getJSONArray("joins");

        for (int i = 0; i < joinsJson.length(); i++) {
            JSONObject joinJson = joinsJson.getJSONObject(i);
            String tableLeft = joinJson.getString("table_left");
            String entityLeft = joinJson.getString("entity_left");
            String tableRight = joinJson.getString("table_right");
            String entityRight = joinJson.getString("entity_right");
            String type = joinJson.getString("type");
            joins.add(new Join(tableLeft, entityLeft, tableRight, entityRight, type));
        }
    }

    public void parseVariables() {
        JSONObject json = readJson();
        JSONArray variablesJson = json.getJSONArray("variables");

        for (int i = 0; i < variablesJson.length(); i++) {
            JSONObject joinJson = variablesJson.getJSONObject(i);
            String tableLeft = joinJson.getString("table_left");
            String entityLeft = joinJson.getString("entity_left");
            String tableRight = joinJson.getString("table_right");
            String entityRight = joinJson.getString("entity_right");
            String type = joinJson.getString("type");
            joins.add(new Join(tableLeft, entityLeft, tableRight, entityRight, type));
        }
    }
}
