package com.inspur.log.demo.logdemo.jdbc;

/**
 * 数据库类型的枚举类.
 * 与DBTypeEnum常量保持一致
 */
public enum DBTypeEnum {

    //关系型数据库
    Generic("Generic", "JDBC", "Generic"),
    Oracle("Oracle", "JDBC", "Oracle"),
    Mysql("Mysql", "JDBC", "Mysql"),
    SQLServer("SQLServer", "JDBC", "SQL Server"),
    DB2("DB2", "JDBC", "DB2"),
    PostgreSQL("PostgreSQL", "JDBC", "PostgreSQL"),
    Greenplum("Greenplum", "JDBC", "Greenplum"),
    Kingbase("Kingbase", "JDBC", "Kingbase"),
    OSCAR("OSCAR", "JDBC", "OSCAR"),
    NewSQL("NewSQL", "JDBC", "NewSQL"),
    DM("DM", "JDBC", "DM"),

    //传统的文件传输系统
    FTP("FTP", "FTP", "FTP"),
    SFTP("SFTP", "SFTP", "SFTP"),
    Host("Host", "Host", "Host"),


    //大数据类型的存储
    Ozone("Ozone", "Ozone", "Ozone"),
    JanusGraph("JanusGraph", "JanusGraph", "JanusGraph"),
    HDFS("HDFS", "HDFS", "HDFS"),
    ElasticSearch("ElasticSearch", "ElasticSearch", "ElasticSearch"),
    HBase("HBase", "HBase", "HBase"),
    Kafka("Kafka", "Kafka", "Kafka"),
    Hive("Hive", "Hive", "Hive"),
    SQL("SQL", "SQL", "SQL"),
    BIGSQL("BIGSQL","BIGSQL","BIGSQL"),
    SPARKSQL("SPARKSQL","SPARKSQL","SPARKSQL"),
    SPARKTASK("SPARKTASK","SPARKTASK","SPARKTASK"),
    MAPREDUCETASK("MAPREDUCETASK","MAPREDUCETASK","MAPREDUCETASK");

    private DBTypeEnum(final String name, final String type, final String text) {
        this.name = name;
        this.type = type;
        this.text = text;
    }

    private final String name;
    private final String type;
    private final String text;

    public String value() {
        return name;
    }

    public String type() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getText() {
        return text;
    }

    public static DBTypeEnum getDBTypeIgnoreCase(String dbType) {
        DBTypeEnum res = null;
        for (DBTypeEnum dbTypeEnum : DBTypeEnum.values()) {
            if (dbTypeEnum.getName().equalsIgnoreCase(dbType)) {
                res = dbTypeEnum;
                break;
            }
        }
        return res;
    }
}
