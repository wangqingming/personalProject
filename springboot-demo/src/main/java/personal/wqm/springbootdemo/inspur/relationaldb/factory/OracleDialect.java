package personal.wqm.springbootdemo.inspur.relationaldb.factory;


public class OracleDialect extends SqlDialect {


    @Override
    String getDefaultStringType() {
        return "varchar2";
    }

    @Override
    String getDefaultBooleanType() {
        return "varchar2";
    }

    @Override
    String getDefaultShortType() {
        return "number";
    }

    @Override
    String getDefaultIntegerType() {
        return "number";
    }

    @Override
    String getDefaultLongType() {
        return "number";
    }

    @Override
    String getDefaultFloatType() {
        return "number";
    }

    @Override
    String getDefaultDoubleType() {
        return "number";
    }

    @Override
    String getDefaultDecimalType() {
        return "number";
    }

    @Override
    String getDefaultDateType() {
        return "date";
    }

    @Override
    String getDefaultTimeType() {
        return "date";
    }

    @Override
    String getDefaultTimestampType() {
        return "timestamp";
    }

    @Override
    String getDefaultBinaryType() {
        return "blob";
    }
}
