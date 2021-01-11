package com.inspur.httpdemo;

/**
 * 数据库类型的枚举类.
 * 与DBTypeEnum常量保持一致
 */
public enum GenericDataType {

    /**
     * 字符串类型
     */
    STRING("string"),

    /**
     * 数字类型
     */
    NUMBER("number"),

    /**
     * 整形，数字类型的子类型
     */
    INTEGER("integer"),

    /**
     * short类型，数字类型的子类型
     */
    SHORT("short"),

    /**
     * long类型，数字类型的子类型
     */
    LONG("long"),


    /**
     * float类型，数字类型的子类型
     */
    FLOAT("float"),

    /**
     * double类型，数字类型的子类型
     */
    DOUBLE("double"),

    /**
     * decimal类型
     */
    DECIMAL("decimal"),

    /**
     * 日期类型
     */
    DATE("date"),
    /**
     * 时间戳类型
     */
    TIMESTAMP("TIMESTAMP"),

    /**
     * boolean类型
     */
    BOOLEAN("boolean"),

    /**
     * 大字符串类型，通常string类型无法表达了，长度过长了，比如oracle中的clob，mysql中的text
     */
    TEXT("text"),

    /**
     * 二进制数据
     */
    BINARY("binary");

    private GenericDataType(final String type) {
        this.type = type;

    }

    private final String type;

    public String type() {
        return type;
    }


}
