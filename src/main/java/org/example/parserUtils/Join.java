package org.example.parserUtils;

public class Join {
    private final String tableLeft;
    private final String entityLeft;
    private final String tableRight;
    private final String entityRight;
    private final String type;

    public Join(String tableLeft, String entityLeft, String tableRight, String entityRight, String type) {
        this.tableLeft = tableLeft;
        this.entityLeft = entityLeft;
        this.tableRight = tableRight;
        this.entityRight = entityRight;
        this.type = type;
    }

    public String getTableLeft() {
        return tableLeft;
    }

    public String getEntityLeft() {
        return entityLeft;
    }

    public String getTableRight() {
        return tableRight;
    }

    public String getEntityRight() {
        return entityRight;
    }

    public String getType() {
        return type;
    }
}
