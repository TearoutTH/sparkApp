package org.example.parserUtils;

public class Parameter {
    private final String name;
    private final String value;
    private final String type;

    public Parameter(String name, String value, String type) {
        this.name = name;
        this.value = value;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public String getType() {
        return type;
    }
}
