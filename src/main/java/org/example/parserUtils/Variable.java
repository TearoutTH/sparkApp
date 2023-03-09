package org.example.parserUtils;

public class Variable {
    private final String name;
    private final String table;
    private final String field;
    private final String type;

    public Variable(String name, String table, String field, String type) {
        this.name = name;
        this.table = table;
        this.field = field;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public String getTable() {
        return table;
    }

    public String getField() {
        return field;
    }

    public String getType() {
        return type;
    }
}
