package personal.wqm.springbootdemo.inspur.relationaldb.factory;

public class MysqlDialect extends SqlDialect {


    @Override
    String getDefaultStringType() {
        return "varchar";
    }

    @Override
    String getDefaultBooleanType() {
        return "tinyint";
    }

    @Override
    String getDefaultShortType() {
        return "SMALLINT";
    }

    @Override
    String getDefaultIntegerType() {
        return "int";
    }

    @Override
    String getDefaultLongType() {
        return "bigint";
    }

    @Override
    String getDefaultFloatType() {
        return "float";
    }

    @Override
    String getDefaultDoubleType() {
        return "double";
    }

    @Override
    String getDefaultDecimalType() {
        return "decimal";
    }

    @Override
    String getDefaultDateType() {
        return "date";
    }

    @Override
    String getDefaultTimeType() {
        return "datetime";
    }

    @Override
    String getDefaultTimestampType() {
        return "timestamp";
    }

    @Override
    String getDefaultBinaryType() {
        return "text";
    }
}
