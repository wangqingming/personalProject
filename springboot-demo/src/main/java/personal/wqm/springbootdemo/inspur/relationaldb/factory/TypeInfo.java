package personal.wqm.springbootdemo.inspur.relationaldb.factory;

public class TypeInfo {
    int typeValue;
    String typeName;
    int typeLength;


    public int getTypeValue() {
        return typeValue;
    }

    public void setTypeValue(int typeValue) {
        this.typeValue = typeValue;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public int getTypeLength() {
        return typeLength;
    }

    public void setTypeLength(int typeLength) {
        this.typeLength = typeLength;
    }

    @Override
    public String toString() {
        return "TypeInfo{" +
                "typeValue=" + typeValue +
                ", typeName='" + typeName + '\'' +
                ", typeLength=" + typeLength +
                '}';
    }
}
