package personal.wqm.springbootdemo.inspur.relationaldb.factory;

import static java.sql.Types.*;

public abstract class SqlDialect {

    public TypeInfo typeConvert(int type, int length) {

        TypeInfo typeInfo = new TypeInfo();
        String typeName;
        switch (type) {
            case CHAR:
            case LONGNVARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case VARCHAR:
            case CLOB:
            case NCLOB:
                typeName = getDefaultStringType();
                break;

            case BIT:
                typeName = getDefaultStringType();

            case BOOLEAN:
                typeName = getDefaultBooleanType();

                break;

            case INTEGER:

                typeName = getDefaultIntegerType();
                break;

            case SMALLINT:
            case TINYINT:
                typeName = getDefaultShortType();

                break;

            case BIGINT:

                typeName = getDefaultLongType();
                break;


            case ROWID:
                typeName = getDefaultStringType();

                break;

            case FLOAT:
            case REAL:
            case 100: //Oracle's BINARY_FLOAT

                typeName = getDefaultFloatType();

                break;

            case DOUBLE:
            case 101://Oracle's BINARY_DOUBLE
                typeName = getDefaultDoubleType();
                break;

            // Since Avro 1.8, LogicalType is supported.
            case DECIMAL:
            case NUMERIC:

                typeName = getDefaultDecimalType();
                break;

            case DATE:

                typeName = getDefaultDateType();

                break;

            case TIME:
                typeName = getDefaultTimeType();

                break;

            case TIMESTAMP:
            case TIMESTAMP_WITH_TIMEZONE:
            case -101: // Oracle's TIMESTAMP WITH TIME ZONE
            case -102: // Oracle's TIMESTAMP WITH LOCAL TIME ZONE

                typeName = getDefaultTimestampType();
                break;

            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case ARRAY:
            case BLOB:
                typeName = getDefaultBinaryType();
                break;

            default:
                typeName = getDefaultStringType();


        }
        typeInfo.setTypeLength(length);
        typeInfo.setTypeName(typeName);
        typeInfo.setTypeValue(type);
        return typeInfo;

    }


    abstract String getDefaultStringType();

    abstract String getDefaultBooleanType();

    abstract String getDefaultShortType();

    abstract String getDefaultIntegerType();

    abstract String getDefaultLongType();

    abstract String getDefaultFloatType();

    abstract String getDefaultDoubleType();

    abstract String getDefaultDecimalType();

    abstract String getDefaultDateType();

    abstract String getDefaultTimeType();

    abstract String getDefaultTimestampType();

    abstract String getDefaultBinaryType();

}
