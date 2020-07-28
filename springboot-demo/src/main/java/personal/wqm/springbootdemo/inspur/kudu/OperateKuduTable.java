package personal.wqm.springbootdemo.inspur.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.client.KuduClient.KuduClientBuilder;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.client.KuduScanToken.KuduScanTokenBuilder;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;
import org.apache.kudu.client.SessionConfiguration.FlushMode;

import java.math.BigDecimal;
import java.math.MathContext;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

import static org.apache.kudu.client.SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC;

/**
 * @Auther: wangqingming
 * @Date: 2018/11/6 09:34
 * @Description:
 */
public class OperateKuduTable {
     protected static Map<String, List<ColumnSchema>> pkMap = new HashMap<>();


    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static Logger logger = Logger.getLogger("CreateKuduTable.class");
//	private static KuduClient client = new KuduClientBuilder("172.16.3.35:7061,172.16.3.38:7061,172.16.12.92:7061")
//			.build();

//	private static KuduClient client = new KuduClientBuilder("10.19.27.11:7051,10.19.27.11:7051,10.19.27.11:7051")
//			.build();


//    private static KuduClient client = new KuduClientBuilder("10.19.27.11:7051,10.19.27.12:7051,10.19.27.13:7051")
//            .build();

    private static KuduClient client = new KuduClientBuilder("172.19.221.4:7051,172.19.221.5:7051,172.19.221.6:7051")
            .build();


    //	private static KuduClient client = new KuduClientBuilder("host2:7051,host3:7051,host4:7051")
//			.build();
    private static int TABLET_NUMBER = 2;

    public static void main(String[] args) throws Exception {
        KuduTable kuduTable = client.openTable("impala::ds_topic2.LAB_SUBITEM");
        List<ColumnSchema> primaryKeyColumns =  getPrimaryKeyColunms( kuduTable);

        System.out.println(primaryKeyColumns);
    }

    protected static void upsertKudu() {
        final KuduSession kuduSession = getKuduSession();
        KuduTable kuduTable = null;
        try {
            kuduTable = client.openTable("test_wqm");
            Operation oper = upsertRecordToKudu(kuduTable);
            kuduSession.apply(oper);
        } catch (KuduException e) {
            e.printStackTrace();
        }


    }
    private static List<ColumnSchema> getPrimaryKeyColunms(KuduTable kuduTable) {

        String tableName = kuduTable.getName();
        if (pkMap.get(tableName) == null) {
            List<ColumnSchema> primaryKeyColumns = kuduTable.getSchema().getPrimaryKeyColumns();
            pkMap.put(tableName, primaryKeyColumns);

        }
        return pkMap.get(tableName);
    }


    protected static KuduSession getKuduSession() {

        KuduSession kuduSession = client.newSession();

        kuduSession.setMutationBufferSpace(1000);
        kuduSession.setFlushMode(AUTO_FLUSH_SYNC);


        kuduSession.setIgnoreAllDuplicateRows(true);


        return kuduSession;
    }

    protected static Upsert upsertRecordToKudu(KuduTable kuduTable) {

        Upsert upsert = kuduTable.newUpsert();
        insert(kuduTable, upsert);


        return upsert;
    }

    private static void insert(KuduTable kuduTable, Operation operation) {
        PartialRow row = operation.getRow();
        Schema colSchema = kuduTable.getSchema();

        List<ColumnSchema> columnSchemas = colSchema.getColumns();
        for (ColumnSchema schema1 : columnSchemas) {
            Type colType = schema1.getType();
            String colname = schema1.getName();

            switch (colType.getDataType(schema1.getTypeAttributes())) {
                case BOOL:
                    row.addBoolean(colname, true);
                    break;
                case FLOAT:
                    row.addFloat(colname, 10f);
                    break;
                case DOUBLE:
                    row.addDouble(colname, 1000d);
                    break;
                case BINARY:
                    row.addBinary(colname, "".getBytes());
                    break;
                case INT8:
                    row.addByte(colname, new Integer(16).byteValue());
                    break;
                case INT16:
                    row.addShort(colname, new Integer(8).shortValue());
                    break;
                case INT32:
                    row.addInt(colname, 32);
                    break;
                case INT64:
                   // row.addLong(colname, 64lL);
                    break;
                case STRING:
                    row.addString(colname, "string");
                    break;
                case UNIXTIME_MICROS:

                    row.addLong(colname, System.currentTimeMillis());
                    break;
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:

                    row.addDecimal(colname, new BigDecimal(128, new MathContext(21)).setScale(18));
                    break;
                default:
                    throw new IllegalStateException(String.format("unknown column type %s", colType));
            }
        }

    }


    /**
     * 创建一个与oracle表结构相同的kudu表：字段相同 ，结构相同
     * 字段与oracle相同,oracle中的字符转换成string,date与TIMESTAMP转换成Type.UNIXTIME_MICROS，数字转换成double
     *
     * @param oracleTableName oralce
     * @param kuduTableName
     * @throws KuduException
     */
    static void createTableAsOracle(String oracleTableName, String kuduTableName) throws KuduException {
        logger.info("创建表:" + kuduTableName);
        // 建表
        ColumnSchemaBuilder csb0 = new ColumnSchemaBuilder("ID", Type.STRING);
        csb0.nullable(false);
        csb0.key(true);

        List<ColumnSchema> columns = new ArrayList<>();
        ColumnSchema rokeyColumnSchema = csb0.build();
        columns.add(rokeyColumnSchema);

        List<Map<String, String>> tableInfos = getTableInfo(oracleTableName);
        for (Map<String, String> tableInfo : tableInfos) {

            ColumnSchema column = createSingleColumn(tableInfo);
            if (column == null) {
                continue;
            }
            columns.add(column);

        }

        ColumnSchemaBuilder timestampColumn = new ColumnSchemaBuilder("TIMESTAMP", Type.INT64);
        timestampColumn.nullable(true);
        ColumnSchema columnSchema = timestampColumn.build();

        columns.add(columnSchema);

        Schema schema = new Schema(columns);
        CreateTableOptions cto = new CreateTableOptions();

        List<String> partitionColumns = new ArrayList<>();
        partitionColumns.add("ID");

        cto.addHashPartitions(partitionColumns, TABLET_NUMBER);

        client.createTable(kuduTableName, schema, cto);
        logger.info("创建表 end:" + kuduTableName);
    }

    static ColumnSchema createSingleColumn(Map<String, String> tableInfo) {

        Iterator iter = tableInfo.entrySet().iterator();
        ColumnSchemaBuilder column = null;
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            Object key = entry.getKey();
            Object val = entry.getValue();

            if ("ID".equals(key)) {
                continue;
            }


            String name = String.valueOf(key);
            String type = String.valueOf(val);


//			column = new ColumnSchemaBuilder(name, Type.STRING);

            if (type.startsWith("VARCHAR") || type.startsWith("CHAR")) {
                column = new ColumnSchemaBuilder(name, Type.STRING);
            } else if (type.startsWith("TIMESTAMP") || type.startsWith("DATE")) {
                column = new ColumnSchemaBuilder(name, Type.UNIXTIME_MICROS);

            } else if (type.startsWith("LONG")) {
                column = new ColumnSchemaBuilder(name, Type.INT64);


            } else if (type.startsWith("NUMBER(8)")) {
                column = new ColumnSchemaBuilder(name, Type.INT8);

            } else if (type.startsWith("NUMBER(16)")) {
                column = new ColumnSchemaBuilder(name, Type.INT16);


            } else if (type.startsWith("INTEGER")) {
                column = new ColumnSchemaBuilder(name, Type.INT32);


            } else if (type.startsWith("NUMBER")) {
                column = new ColumnSchemaBuilder(name, Type.DOUBLE);


            } else {
                System.out.println("columnName " + name + ", type " + type);
                System.exit(0);

            }
            column.nullable(true);
            ColumnSchema columnSchema = column.build();
            return columnSchema;

        }

        return null;


    }

    public static List<Map<String, String>> getTableInfo(String tableName) {

//        Connection sourceConn = null;
//        List<Map<String, String>> tableInfos = new ArrayList<Map<String, String>>();
//        try {
//            sourceConn = JdbcUtils.getOracleConnection();
//            Statement statement = sourceConn.createStatement();
//            String logMemberSQL = "select * from " + tableName;
//
//            ResultSet logMemberResultSet = null;
//            logMemberResultSet = statement.executeQuery(logMemberSQL);
//            ResultSetMetaData resultMetaData = logMemberResultSet.getMetaData();
//            int cols = resultMetaData.getColumnCount();
//
//            for (int i = 1; i <= cols; i++) {
//                Map<String, String> tableInfo = new HashMap<String, String>();
//                String columnName = resultMetaData.getColumnName(i);
//                String columnTypeName = resultMetaData.getColumnTypeName(i);
//                tableInfo.put(columnName, columnTypeName);
//
//                tableInfos.add(tableInfo);
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//
//        } finally {
//
//        }

        return null;

    }

    // 创建表
    static void createTable2(KuduClient client, String tblName) throws KuduException {
        logger.info("创建表:" + tblName);
        // 建表

        ColumnSchemaBuilder csb1 = new ColumnSchemaBuilder("ID", Type.STRING);
        csb1.nullable(false);
        csb1.key(true);


        ColumnSchemaBuilder csb2 = new ColumnSchemaBuilder("INT8", Type.INT8);
        csb2.nullable(true);

        ColumnSchemaBuilder csb3 = new ColumnSchemaBuilder("INT16", Type.INT16);
        csb3.nullable(true);

        ColumnSchemaBuilder csb4 = new ColumnSchemaBuilder("INT32", Type.INT32);
        csb4.nullable(true);

        ColumnSchemaBuilder csb5 = new ColumnSchemaBuilder("INT64", Type.INT64);
        csb5.nullable(true);

        ColumnSchemaBuilder csb6 = new ColumnSchemaBuilder("FLOATS", Type.FLOAT);
        csb6.nullable(true);


        ColumnSchemaBuilder csb7 = new ColumnSchemaBuilder("DOUBLES", Type.DOUBLE);
        csb7.nullable(true);


        ColumnSchemaBuilder csb8 = new ColumnSchemaBuilder("DECIMALS", Type.DECIMAL);
        csb8.nullable(true);

        ;
        csb8.typeAttributes(new ColumnTypeAttributes.ColumnTypeAttributesBuilder().precision(21).scale(18).build());

        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(csb1.build());
        columns.add(csb2.build());
        columns.add(csb3.build());
        columns.add(csb4.build());
        columns.add(csb5.build());
        columns.add(csb6.build());
        columns.add(csb7.build());
        columns.add(csb8.build());

        Schema schema = new Schema(columns);
        CreateTableOptions cto = new CreateTableOptions();

        List<String> partitionColumns = new ArrayList<>();
        partitionColumns.add("ID");

        cto.addHashPartitions(partitionColumns, 10);
        // cto.setNumReplicas(3);

        KuduTable table1 = client.createTable(tblName, schema, cto);
        logger.info("创建表 end:" + tblName);
    }


    // 创建表
    static void createTable(KuduClient client, String tblName) throws KuduException {
        logger.info("创建表:" + tblName);
        // 建表
        ColumnSchemaBuilder csb0 = new ColumnSchemaBuilder("ROWID", Type.STRING);
        csb0.nullable(false);
        csb0.key(true);

        ColumnSchemaBuilder csb1 = new ColumnSchemaBuilder("ID", Type.STRING);
        csb1.nullable(true);
        ColumnSchemaBuilder csb2 = new ColumnSchemaBuilder("DISPATCHED_OFFICES_NUMBER;", Type.DOUBLE);
        csb2.nullable(true);
        ColumnSchemaBuilder csb3 = new ColumnSchemaBuilder("PREPARATION_NUMBER;", Type.DOUBLE);
        csb3.nullable(true);
        ColumnSchemaBuilder csb4 = new ColumnSchemaBuilder("CIVIL_SERVANT;", Type.DOUBLE);
        csb4.nullable(true);
        ColumnSchemaBuilder csb5 = new ColumnSchemaBuilder("CIVIL_SERVANT_ADMIN;", Type.DOUBLE);
        csb5.nullable(true);
        ColumnSchemaBuilder csb6 = new ColumnSchemaBuilder("CAREER_PREPARATION;", Type.DOUBLE);
        csb6.nullable(true);
        ColumnSchemaBuilder csb7 = new ColumnSchemaBuilder("YEAR_END_RENTAL_AREA;", Type.DOUBLE);
        csb7.nullable(true);
        ColumnSchemaBuilder csb8 = new ColumnSchemaBuilder("BUSINESS_PREMISES_AREA;", Type.DOUBLE);
        csb8.nullable(true);
        ColumnSchemaBuilder csb9 = new ColumnSchemaBuilder("HOUSE_RENT;", Type.DOUBLE);
        csb9.nullable(true);
        ColumnSchemaBuilder csb10 = new ColumnSchemaBuilder("WHETHER_BRANCH;", Type.STRING);
        csb10.nullable(true);
        ColumnSchemaBuilder csb11 = new ColumnSchemaBuilder("NATIONAL_AUTO_AREA;", Type.STRING);
        csb11.nullable(true);
        ColumnSchemaBuilder csb12 = new ColumnSchemaBuilder("REACH_STANDARD_INFR_CON;", Type.STRING);
        csb12.nullable(true);
        ColumnSchemaBuilder csb13 = new ColumnSchemaBuilder("INDEPENDENT_ACCOUNTING;", Type.STRING);
        csb13.nullable(true);
        ColumnSchemaBuilder csb14 = new ColumnSchemaBuilder("NON_INDE_ACC_AFF_UNITS;", Type.STRING);
        csb14.nullable(true);
        ColumnSchemaBuilder csb15 = new ColumnSchemaBuilder("ESTABLISHMENT_DATE;", Type.STRING);
        csb15.nullable(true);
        ColumnSchemaBuilder csb16 = new ColumnSchemaBuilder("PHONE_NUMBER;", Type.STRING);
        csb16.nullable(true);
        ColumnSchemaBuilder csb17 = new ColumnSchemaBuilder("HIGHER_AUTHORITY_NAME;", Type.STRING);
        csb17.nullable(true);
        ColumnSchemaBuilder csb18 = new ColumnSchemaBuilder("HIGHER_AUTHORITY_CODE;", Type.STRING);
        csb18.nullable(true);
        ColumnSchemaBuilder csb19 = new ColumnSchemaBuilder("DISTINCT_CODE;", Type.STRING);
        csb19.nullable(true);
        ColumnSchemaBuilder csb20 = new ColumnSchemaBuilder("REG_ECONOMIC_TYPE_CODE;", Type.STRING);
        csb20.nullable(true);
        ColumnSchemaBuilder csb21 = new ColumnSchemaBuilder("HEALTH_ORG_CODE;", Type.STRING);
        csb21.nullable(true);
        ColumnSchemaBuilder csb22 = new ColumnSchemaBuilder("ORG_CLASS_MANAGER_CODE;", Type.STRING);
        csb22.nullable(true);
        ColumnSchemaBuilder csb23 = new ColumnSchemaBuilder("HOST_UNIT_CODE;", Type.STRING);
        csb23.nullable(true);
        ColumnSchemaBuilder csb24 = new ColumnSchemaBuilder("UNIT_RELATION_CODE;", Type.STRING);
        csb24.nullable(true);
        ColumnSchemaBuilder csb25 = new ColumnSchemaBuilder("LEGAL_REP;", Type.STRING);
        csb25.nullable(true);
        ColumnSchemaBuilder csb26 = new ColumnSchemaBuilder("INSTITUTION_ADDRESS;", Type.STRING);
        csb26.nullable(true);
        ColumnSchemaBuilder csb27 = new ColumnSchemaBuilder("ORG_LONGITUDE_COORDINATES;", Type.DOUBLE);
        csb27.nullable(true);
        ColumnSchemaBuilder csb28 = new ColumnSchemaBuilder("ORG_LATITUDE_COORDINATES;", Type.DOUBLE);
        csb28.nullable(true);
        ColumnSchemaBuilder csb29 = new ColumnSchemaBuilder("TOWN_STREET_NAME;", Type.STRING);
        csb29.nullable(true);
        ColumnSchemaBuilder csb30 = new ColumnSchemaBuilder("TOWN_STREET_CODE;", Type.STRING);
        csb30.nullable(true);
        ColumnSchemaBuilder csb31 = new ColumnSchemaBuilder("POSTCODE;", Type.STRING);
        csb31.nullable(true);
        ColumnSchemaBuilder csb32 = new ColumnSchemaBuilder("E_MAIL;", Type.STRING);
        csb32.nullable(true);
        ColumnSchemaBuilder csb33 = new ColumnSchemaBuilder("URL;", Type.STRING);
        csb33.nullable(true);
        ColumnSchemaBuilder csb34 = new ColumnSchemaBuilder("HEALTH_SUPERVISION_ORG_CODE;", Type.STRING);
        csb34.nullable(true);
        ColumnSchemaBuilder csb35 = new ColumnSchemaBuilder("ORG_CLASS_MAN_CODE;", Type.STRING);
        csb35.nullable(true);
        ColumnSchemaBuilder csb36 = new ColumnSchemaBuilder("ACQUISITION_MECHANISM_CODE;", Type.STRING);
        csb36.nullable(true);
        ColumnSchemaBuilder csb37 = new ColumnSchemaBuilder("ACQUISITION_MECHANISM_NAME;", Type.STRING);
        csb37.nullable(true);
        ColumnSchemaBuilder csb38 = new ColumnSchemaBuilder("COLLECTION_PERSONNEL_CODE;", Type.STRING);
        csb38.nullable(true);
        ColumnSchemaBuilder csb39 = new ColumnSchemaBuilder("DATA_AUDIT;", Type.STRING);
        csb39.nullable(true);
        ColumnSchemaBuilder csb40 = new ColumnSchemaBuilder("DATA_AUDIT_ORG_CODE;", Type.STRING);
        csb40.nullable(true);
        ColumnSchemaBuilder csb41 = new ColumnSchemaBuilder("DATA_AUDIT_PERSONNEL_CODE;", Type.STRING);
        csb41.nullable(true);
        ColumnSchemaBuilder csb42 = new ColumnSchemaBuilder("ORG_NAME;", Type.STRING);
        csb42.nullable(true);
        ColumnSchemaBuilder csb43 = new ColumnSchemaBuilder("ORG_CODE;", Type.STRING);
        csb43.nullable(true);
        ColumnSchemaBuilder csb44 = new ColumnSchemaBuilder("DATA_AUDIT_TIME;", Type.STRING);
        csb44.nullable(true);

        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(csb0.build());
        columns.add(csb1.build());
        columns.add(csb2.build());
        columns.add(csb3.build());
        columns.add(csb4.build());
        columns.add(csb5.build());
        columns.add(csb6.build());
        columns.add(csb7.build());
        columns.add(csb8.build());
        columns.add(csb9.build());
        columns.add(csb10.build());
        columns.add(csb11.build());
        columns.add(csb12.build());
        columns.add(csb13.build());
        columns.add(csb14.build());
        columns.add(csb15.build());
        columns.add(csb16.build());
        columns.add(csb17.build());
        columns.add(csb18.build());
        columns.add(csb19.build());
        columns.add(csb20.build());
        columns.add(csb21.build());
        columns.add(csb22.build());
        columns.add(csb23.build());
        columns.add(csb24.build());
        columns.add(csb25.build());
        columns.add(csb26.build());
        columns.add(csb27.build());
        columns.add(csb28.build());
        columns.add(csb29.build());
        columns.add(csb30.build());
        columns.add(csb31.build());
        columns.add(csb32.build());
        columns.add(csb33.build());
        columns.add(csb34.build());
        columns.add(csb35.build());
        columns.add(csb36.build());
        columns.add(csb37.build());
        columns.add(csb38.build());
        columns.add(csb39.build());
        columns.add(csb40.build());
        columns.add(csb41.build());
        columns.add(csb42.build());
        columns.add(csb43.build());
        columns.add(csb44.build());

        Schema schema = new Schema(columns);
        CreateTableOptions cto = new CreateTableOptions();

        List<String> partitionColumns = new ArrayList<>();
        partitionColumns.add("ROWID");

        cto.addHashPartitions(partitionColumns, 10);
        // cto.setNumReplicas(3);

        KuduTable table1 = client.createTable(tblName, schema, cto);
        logger.info("创建表 end:" + tblName);
    }

    // 增加列
    static void insertColumn(KuduClient client, String tblName) throws KuduException {
        logger.info("增加表字段:" + tblName);
        AlterTableOptions ato = new AlterTableOptions();
        AlterTableOptions ato1 = ato.addColumn("d", Type.INT32, 0);
        AlterTableResponse rtv = client.alterTable(tblName, ato);
        logger.info("\t" + rtv.toString());
        logger.info("增加表字段 end:" + tblName);
    }

    // 删除列
    static void deleteColumn(KuduClient client, String tblName) throws KuduException {
        logger.info("删除表字段:" + tblName);

        AlterTableOptions ato = new AlterTableOptions();
        AlterTableOptions ato1 = ato.dropColumn("c");
        AlterTableResponse rtv = client.alterTable(tblName, ato);
        logger.info("\t" + rtv.toString());
        logger.info("删除表字段 end:" + tblName);
    }

    // 增加数据
    static void insertData(KuduClient client, String tblName) throws KuduException {
        logger.info("增加数据:" + tblName);
        KuduTable table = client.openTable(tblName);

        KuduSession session = client.newSession();
        session.setFlushMode(FlushMode.MANUAL_FLUSH);

        for (int i = 0; i < 20; i++) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addString("name", Integer.toHexString(i));
            row.addInt("age", i);
            row.addString("city", Integer.toHexString(i));
            session.apply(insert);
        }

        session.flush();
        session.close();
        logger.info("增加数据 end:" + tblName);
    }


    // 增加数据
    static void insertKudu(KuduClient client, String tblName) throws KuduException {
        logger.info("增加数据:" + tblName);
        KuduTable table = client.openTable(tblName);

        table.getSchema();

        KuduSession session = client.newSession();
        //session.setFlushMode(FlushMode.MANUAL_FLUSH);


        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addString("fxxx_bh", "0129671fdfd149d4a0a27909ecf890cf");

        //row.addString("shsj", "2017-07-26 17:33:07.0");

        row.addString("sh_ry_dm", "24101810012");

        row.addString("sh_ry_xm", "贺彬");

        row.addString("sp_swjg_mc", "河南省巩义市地方税务局");
        row.addLong("shsj", System.currentTimeMillis());
        row.addLong("sjgl_tbsj", System.currentTimeMillis());


//		for (int i = 0; i < 2000; i++) {
//			Insert insert = table.newInsert();
//			PartialRow row = insert.getRow();
//			row.addString("ROWID", Integer.toHexString(i));
//
//			row.addDouble("ID", Double.valueOf(i));
//
//			row.addString("OWNER", "OWNER" + i);
//
//			row.addString("OBJECT_NAME", "OBJECT_NAME" + i);
//
//			row.addString("SUBOBJECT_NAME","SUBOBJECT_NAME" + i);
//			row.addLong("DATES", System.currentTimeMillis());
//			row.addLong("TIMESTAMP", System.currentTimeMillis());
//
//			if (i % 10000 == 0) {
//
//				System.out.println(i+ "条数据已经插入");
//			}
//			session.apply(insert);
//		}

        session.flush();
        session.close();
        logger.info("增加数据 end:" + tblName);
    }


    // 查询数据
    static void queryData(KuduClient client, String tblName) throws KuduException {
        long starTime = System.currentTimeMillis();
        logger.info("查询数据 :" + tblName);
        KuduTable table = client.openTable(tblName);
        KuduScannerBuilder scanBuilder = client.newScannerBuilder(table);
        KuduScanner scan = scanBuilder.build();
        Schema schema = table.getSchema();
        List<ColumnSchema> columnSchemas = schema.getColumns();

        int count = 0;

        List<Map<String, Object>> avrosMap = new ArrayList<Map<String, Object>>();

        while (scan.hasMoreRows()) {


            RowResultIterator iterator = scan.nextRows();
            while (iterator.hasNext()) {
                Map<String, Object> map = new HashMap<String, Object>();

                count++;
                RowResult rs = iterator.next();
                for (int i = 0; i < columnSchemas.size(); i++) {

                    String typeName = columnSchemas.get(i).getType().getName();
                    String columnName = columnSchemas.get(i).getName();

                    if ("SRCROWID".equals(columnName) && rs.isNull(i)) {
                        System.out.println("SRCROWID  is null");

                    }
//
//
//
                    if (rs.isNull(i)) {
                        continue;
                    }
                    // Need to handle CLOB and BLOB before getObject() is called, due to ResultSet's
                    // maximum portability statement
                    if ("int8".equals(typeName)) {
                        map.put(columnName, rs.getByte(i));
                        continue;
                    }

                    // Need to handle CLOB and BLOB before getObject() is called, due to ResultSet's
                    // maximum portability statement
                    if ("int16".equals(typeName)) {
                        map.put(columnName, rs.getShort(i));
                        continue;
                    }

                    // Need to handle CLOB and BLOB before getObject() is called, due to ResultSet's
                    // maximum portability statement
                    if ("int32".equals(typeName)) {
                        map.put(columnName, rs.getInt(i));
                        continue;
                    }

                    // Need to handle CLOB and BLOB before getObject() is called, due to ResultSet's
                    // maximum portability statement
                    if ("int64".equals(typeName)) {
                        map.put(columnName, rs.getLong(i));
                        continue;
                    }
                    // Need to handle CLOB and BLOB before getObject() is called, due to ResultSet's
                    // maximum portability statement
                    if ("float".equals(typeName)) {
                        map.put(columnName, rs.getFloat(i));
                        continue;
                    }

                    // Need to handle CLOB and BLOB before getObject() is called, due to ResultSet's
                    // maximum portability statement
                    if ("double".equals(typeName)) {
                        map.put(columnName, rs.getDouble(i));
                        continue;
                    }

                    // Need to handle CLOB and BLOB before getObject() is called, due to ResultSet's
                    // maximum portability statement
                    if ("binary".equals(typeName)) {
                        map.put(columnName, rs.getBinary(i));
                        continue;
                    }

                    // Need to handle CLOB and BLOB before getObject() is called, due to ResultSet's
                    // maximum portability statement
                    if ("string".equals(typeName)) {

                        map.put(columnName, rs.getString(i));
                        continue;
                    }

                    // Need to handle CLOB and BLOB before getObject() is called, due to ResultSet's
                    // maximum portability statement
                    if ("bool".equals(typeName)) {
                        map.put(columnName, rs.getBoolean(i));
                        continue;
                    }

                    if ("unixtime_micros".equals(typeName)) {


                        long temp = rs.getLong(i);
                        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
                        Date date = new Date(temp);
                        String dateString = sdf.format(date);
                        map.put(columnName, dateString);
                        continue;
                    }
                    if ("decimal".equals(typeName)) {


                        BigDecimal value = (rs.getDecimal(i));
                        map.put(columnName, value.doubleValue());
                        continue;
                    } else {

                        map.put(columnName, rs.getString(i));
                    }
                }


//				if (count % 1000 == 0) {
//				}
                avrosMap.add(map);

                if (avrosMap.size() % 100 == 0) {
                    System.out.println(avrosMap);
                    avrosMap = new ArrayList<Map<String, Object>>();
                }


//				System.out.println("" + avrosMap);
//
//				if (avrosMap.size()  ==  10) {
//					try {
//						KuduToAvroStream.kuduResultToAvro(avrosMap,columnSchemas,100);
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//
//					break;
//				}
                //System.out.println(avrosMap);


            }


        }
        System.out.println(avrosMap);


        long endTime = System.currentTimeMillis();


        logger.info("查询数据 end:" + tblName + (endTime - starTime));
    }


    // 查询数据
    static void queryData2(KuduClient client, String tblName) throws KuduException {
        long starTime = System.currentTimeMillis();
        logger.info("查询数据 :" + tblName);
        KuduTable table = client.openTable(tblName);
        KuduScannerBuilder scanBuilder = client.newScannerBuilder(table);
        KuduScanner scan = scanBuilder.build();
        Schema schema = table.getSchema();
        List<ColumnSchema> columnSchemas = schema.getColumns();

        int fetchSize = 1000;

        int count = 0;
        List<Map<String, Object>> avrosMaps = new ArrayList<Map<String, Object>>();
        while (scan.hasMoreRows()) {

            RowResultIterator iterator = scan.nextRows();
            while (iterator.hasNext()) {
                count++;
                RowResult rs = iterator.next();
                Map<String, Object> map = new HashMap<String, Object>();

                for (int i = 0; i < columnSchemas.size(); i++) {

                    if (rs.isNull(i)) {
                        continue;
                    }
                    String typeName = columnSchemas.get(i).getType().getName();
                    String columnName = columnSchemas.get(i).getName();


                    if ("int8".equals(typeName)) {
                        map.put(columnName, rs.getByte(i));
                        continue;
                    }

                    if ("int16".equals(typeName)) {
                        map.put(columnName, rs.getShort(i));
                        continue;
                    }

                    if ("int32".equals(typeName)) {
                        map.put(columnName, rs.getInt(i));
                        continue;
                    }


                    if ("int64".equals(typeName)) {
                        map.put(columnName, rs.getLong(i));
                        continue;
                    }

                    if ("float".equals(typeName)) {
                        map.put(columnName, rs.getFloat(i));
                        continue;
                    }

                    if ("double".equals(typeName) || "decimal".equalsIgnoreCase(typeName)) {
                        map.put(columnName, rs.getDouble(i));
                        continue;
                    }


                    if ("binary".equals(typeName)) {
                        map.put(columnName, rs.getBinary(i));
                        continue;
                    }

                    if ("string".equals(typeName)) {
                        map.put(columnName, rs.getString(i));
                        continue;
                    }

                    if ("bool".equals(typeName)) {
                        map.put(columnName, rs.getBoolean(i));
                        continue;
                    }

                    if ("unixtime_micros".equals(typeName)) {

                        String dateFormater = "yyyy-MM-dd HH:mm:ss";

                        long temp = rs.getLong(i);
                        SimpleDateFormat sdf = new SimpleDateFormat(dateFormater);
                        Date date = new Date(temp);
                        String dateString = sdf.format(date);
                        map.put(columnName, dateString);
                        continue;
                    } else {

                        map.put(columnName, rs.getString(i));
                    }
                }

                avrosMaps.add(map);

                if (avrosMaps.size() == fetchSize) {

                    avrosMaps = new ArrayList<Map<String, Object>>();


                }
            }
        }


    }


    private static int scan(KuduScanner scan1, int i) throws KuduException {
        while (scan1.hasMoreRows()) {
            RowResultIterator iterator = scan1.nextRows();
            //System.out.println(i++);
            while (iterator.hasNext()) {
                System.out.println(i++);
                RowResult rr = iterator.next();
                // int ccnt = rr.getSchema().getColumnCount();
                // logger.info("column count=" + ccnt);
                System.out.println(rr.getLong("timestamp") + " ---- " + rr.getString("ROWID"));
            }
        }
        return i;
    }

    // 获取所有表名
    static void getAllTables(KuduClient client) throws KuduException {
        ListTablesResponse tables = client.getTablesList();
        List<String> tblNames = tables.getTablesList();

        System.out.println("getalltables---------------");
        for (String s : tblNames) {
            System.out.println(s);
        }
        System.out.println("getalltables==============");
    }

    static void getOneTable(String tablenName) throws KuduException {
        System.out.println("getOneTable");
        KuduTable table = client.openTable(tablenName);
        Schema schema = table.getSchema();
        //PartitionSchema ps = table.getPartitionSchema();
        List<ColumnSchema> columnSchemas = schema.getColumns();
        for (ColumnSchema c : columnSchemas) {
            System.out.println("name=" + c.getName() + ",type=" + c.getType().toString());
        }
    }

    static void createTable2(KuduClient client) throws KuduException {
        /*
         * create table tabx (x int, y string, primary key (x)) partition by " +
         * "hash (x) partitions 3, range (x) (partition values < 1, partition " +
         * "1 <= values < 10, partition 10 <= values < 20, partition value = 30) " +
         * "stored as kudu
         */

        // 建表
        ColumnSchemaBuilder csb1 = new ColumnSchemaBuilder("x", Type.INT32);
        ColumnSchemaBuilder csb2 = new ColumnSchemaBuilder("y", Type.STRING);
        csb1.nullable(false);
        csb1.key(true);

        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(csb1.build());
        columns.add(csb2.build());

        Schema schema = new Schema(columns);
        CreateTableOptions cto = new CreateTableOptions();

        List<String> partitionColumns = new ArrayList<>();
        partitionColumns.add("x");
        cto.addHashPartitions(partitionColumns, 3);

        int[][] ary = {{Integer.MIN_VALUE, 1}, {1, 10}, {10, 20}, {20, Integer.MAX_VALUE}};

        for (int i = 0; i < ary.length; i++) {
            PartialRow prl = new PartialRow(schema);
            PartialRow pru = new PartialRow(schema);
            prl.addInt("x", ary[i][0]);
            pru.addInt("x", ary[i][1]);
            cto.addRangePartition(prl, pru);
        }

        KuduTable table1 = client.createTable("ft2017072101", schema, cto);
    }

    // 删除表
    static void deleteTable(KuduClient client, String tblName) throws KuduException {
        DeleteTableResponse response = client.deleteTable(tblName);
        System.out.println("delete elapsed:" + response.getElapsedMillis());
    }

    // 增删改查数据
    static void dataoperate_insert(KuduClient client) throws KuduException {
        System.out.println("dataoperate_insert");
        if (!client.tableExists("fktest1")) {
            System.out.println("表不存在");
            return;
        }

    }

    static void dataoperate_insert2(KuduClient client) throws KuduException {
        System.out.println("dataoperate_insert");
        if (!client.tableExists("ft2017072101")) {
            System.out.println("表不存在");
            return;
        }

        KuduTable table = client.openTable("ft2017072101");
        KuduSession session = client.newSession();
        session.setFlushMode(FlushMode.MANUAL_FLUSH);

        for (int i = 0; i <= 25; i++) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addInt("x", i);
            row.addString("y", "fangjx-" + i);

            session.apply(insert);
        }

        session.flush();
        session.close();
    }

    static void dataoperate_query(KuduClient client, String tableName) throws KuduException {
        System.out.println("dataoperate_query");
        if (!client.tableExists(tableName)) {
            System.out.println("表不存在");
            return;
        }

        KuduTable table = client.openTable(tableName);

        KuduScannerBuilder scanBuilder1 = client.newScannerBuilder(table);
        ColumnSchema col = table.getSchema().getColumn("timestamp");
        KuduPredicate greater = KuduPredicate.newComparisonPredicate(col, ComparisonOp.GREATER, 1514941445000L);
        // 等于，大于，大于等于，小于，小于等于
        scanBuilder1.addPredicate(greater);


        KuduPredicate LESS = KuduPredicate.newComparisonPredicate(col, ComparisonOp.LESS, 1514948585277l);
        scanBuilder1.addPredicate(LESS);


        KuduScanner scan1 = scanBuilder1.build();

        int count = 0;
        while (scan1.hasMoreRows()) {
            RowResultIterator iterator = scan1.nextRows();
            System.out.println("numRows=" + iterator.getNumRows());
            while (iterator.hasNext()) {
                RowResult rr = iterator.next();
//				Double iv = rr.getString(0);
//                String sv = rr.getString(1);
                System.out.println(rr.getLong("timestamp") + " ---- " + rr.getString("ROWID"));

                System.out.println(count++);
            }
        }
        scan1.close();
    }

    static void dataoperate_delete(KuduClient client) throws KuduException {
        System.out.println("dataoperate_delete");
        if (!client.tableExists("fktest1")) {
            System.out.println("表不存在");
            return;
        }

        KuduTable table = client.openTable("fktest1");

        KuduSession session = client.newSession();
        session.setFlushMode(FlushMode.MANUAL_FLUSH);

        Delete delete = table.newDelete();
        delete.getRow().addInt("id", 3);
        session.apply(delete);

        session.flush();
        session.close();
    }

    //
    static void tableScan(KuduClient client) throws Exception {
        KuduTable table = client.openTable("ft2017072101");
        KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);
        List<String> projectedCols = new ArrayList<>();
        projectedCols.add("x");
        projectedCols.add("y");
        tokenBuilder.setProjectedColumnNames(projectedCols);

        Schema schema = table.getSchema();

        KuduPredicate kp1 = KuduPredicate.newComparisonPredicate(schema.getColumn("x"), ComparisonOp.GREATER, 5);
        KuduPredicate kp2 = KuduPredicate.newComparisonPredicate(schema.getColumn("x"), ComparisonOp.LESS, 15);

        // 等于，大于，大于等于，小于，小于等于
        tokenBuilder.addPredicate(kp1);
        tokenBuilder.addPredicate(kp2);
        List<KuduScanToken> scanTokens = tokenBuilder.build();

        for (KuduScanToken token : scanTokens) {
//            System.out.println("token");
//            LocatedTablet tablet = token.getTablet();
//            List<TScanRangeLocation> locations = Lists.newArrayList();
//            if (tablet.getReplicas().isEmpty()) {
//                System.out.println(String.format("At least one tablet does not have any replicas. Tablet ID: %s",
//                        new String(tablet.getTabletId(), Charsets.UTF_8)));
//            }
//
//            for (LocatedTablet.Replica replica : tablet.getReplicas()) {
//                TNetworkAddress address = new TNetworkAddress(replica.getRpcHost(), replica.getRpcPort());
//                // Use the network address to look up the host in the global list
//                // 输出address
//                System.out.println(
//                        String.format("\treplica.host=%s,port=%d", replica.getRpcHost(), replica.getRpcPort()));
//            }
//
//            KuduScanner scanner = token.intoScanner(client);
//            RowResultIterator iterator = scanner.nextRows();
//            while (iterator.hasNext()) {
//                RowResult result = iterator.next();
//                System.out.println(String.format("\t\t val:x=%d  y=%s", result.getInt("x"), result.getString("y")));
//
//            }
//            scanner.close();
        }
    }


}
