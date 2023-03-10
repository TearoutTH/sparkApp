package org.example.parserUtils;

import java.util.List;

public class Node {
    private final boolean leaf;
    private final String id;
    private final List<String> criteriasId;
    private final String operator;
    private Node trueChild;
    private Node falseChild;
    private final double loss;

    public Node(boolean leaf, String id, List<String> criteriasId, String operator, Node trueChild, Node falseChild, double loss) {
        this.leaf = leaf;
        this.id = id;
        this.criteriasId = criteriasId;
        this.operator = operator;
        this.trueChild = trueChild;
        this.falseChild = falseChild;
        this.loss = loss;
    }

    public void setTrueChild(Node trueChild) {
        this.trueChild = trueChild;
    }

    public void setFalseChild(Node falseChild) {
        this.falseChild = falseChild;
    }

    public boolean isLeaf() {
        return leaf;
    }

    public String getId() {
        return id;
    }

    public List<String> getCriteriasId() {
        return criteriasId;
    }

    public String getOperator() {
        return operator;
    }

    public Node getTrueChild() {
        return trueChild;
    }

    public Node getFalseChild() {
        return falseChild;
    }

    public double getLoss() {
        return loss;
    }

    @Override
    public String toString() {
        return "{ id: " + id + "\n" +
                "criterias: " + criteriasId.toString() + "\n"+
                "operator: " + operator + "\n" +
                "trueChild: " + trueChild.toString() + "\n" +
                "falseChild: " + falseChild.toString() + "\n";
    }
}
