package org.example.parserUtils;

public class Criteria {
    private final String id;
    private final String parameter;
    private final String operator;
    private final String value;

    public Criteria(String id, String parameter, String operator, String value) {
        this.id = id;
        this.parameter = parameter;
        this.operator = operator;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public String getParameter() {
        return parameter;
    }

    public String getOperator() {
        return operator;
    }

    public String getValue() {
        return value;
    }
}
