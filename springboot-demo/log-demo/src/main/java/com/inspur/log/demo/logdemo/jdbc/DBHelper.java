package com.inspur.log.demo.logdemo.jdbc;



import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;

import static java.sql.Types.*;

public class DBHelper {

    public static void main(String[] args) {

        String driverName = "com.kingbase8.Driver";
        String url = "jdbc:kingbase8://10.110.60.51:54321/MINISTRY";
        String user = "MINISTRY";
        String pwd = "MINISTRY";
        String type = "Kingbase";
        String table = "sjk_test_view_by_view";



        DBHelper db = null;
        try {
            db = new DBHelper(url, user, pwd, driverName, type);
            List<Map<String, String>> list = db.getTableColumnsInfo(null, table);

            System.out.println(JSON.toJSONString(list));

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private static final int MAX_DIGITS_IN_BIGINT = 19;
    private static final int MAX_DIGITS_IN_INT = 9;
    // Derived from MySQL default precision.
    private Connection conn = null;
    private PreparedStatement pst = null;
    private Statement st = null;
    private ResultSet rs = null;
    private String type = "";

    public DBHelper(String url, String user, String password, String driverName, String dbtype) throws Exception {
        this.type = dbtype;
        Driver driver;
        try {
            Class driverClass =  Class.forName(driverName);//指定连接类型
            driver = (Driver) driverClass.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("获取数据库驱动失败！【" + driverName + "】");
        }
        Properties props = new Properties();
        if(user != null){
            props.put("user", user);

        }
        //
        if(password != null){
            props.put("password", password);
        }
        try {
            conn = driver.connect(url, props);//获取连接
        } catch (SQLException e) {
            e.printStackTrace();
            throw new Exception("获取数据库连接失败！url=【" + url + "】，user=【" + user + "】,password=【" + password + "】，driverName=【" + driverName + "】");
        }
    }

    public static Connection getConnection(String url, String user, String password, String driverName) throws Exception {


        Class driverClass =  Class.forName(driverName);
        Driver driver = (Driver) driverClass.newInstance();
        Properties props = new Properties();
        if(user != null){
            props.put("user", user);

        }
        //
        if(password != null){
            props.put("password", password);
        }
        return driver.connect(url, props);
       /* Class.forName(driverName);//指定连接类型
        DriverManager.setLoginTimeout(10);
        return DriverManager.getConnection(url, user, password);*/ //获取连接throw  new Exception("获取数据库驱动失败！【"+driverName+"】");
    }

    public static String converthive(int t, ResultSetMetaData meta, int i) {
        String type = "string";
        try {
            switch (t) {
                case CHAR:
                case LONGNVARCHAR:
                case LONGVARCHAR:
                case NCHAR:
                case NVARCHAR:
                case VARCHAR:
                case CLOB:
                case NCLOB:
                    type = "string";
                    break;
                case BIT:
                case BOOLEAN:
                    type = "boolean";
                    break;
                case INTEGER:
                    if (meta.isSigned(i) || (meta.getPrecision(i) > 0 && meta.getPrecision(i) <= MAX_DIGITS_IN_INT)) {
                        type = "int";
                    } else {
                        type = "bigint";
                    }
//                type="bigint";
                    break;
                case SMALLINT:
                case TINYINT:
                    type = "int";
                    break;
                case BIGINT:
                    int precision = meta.getPrecision(i);
                    if (precision < 0 || precision > MAX_DIGITS_IN_BIGINT) {
                        type = "string";
                    } else {
                        type = "bigint";
                    }
//                type="bigint";
                    break;
                case ROWID:
                    type = "string";
                    break;
                case FLOAT:
                case REAL:
                    type = "float";
                    break;
                case DOUBLE:
                    type = "double";
                    break;
                // Since Avro 1.8, LogicalType is supported.
                case DECIMAL:
                case NUMERIC:
//                type="decimal"; hive avro不支持
                    type = "string";
                    break;
                case DATE:
//                type="date";
                    type = "string";
                    break;
                case TIME:
//                type="int";
                    type = "string";
                    break;
                case TIMESTAMP:
//                type="timestamp"; hive avro不支持
                    type = "string";
                    break;

                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                case ARRAY:
                case BLOB:
//                type="binary";  hive avro不支持
                    type = "string";
                    break;
                default:
                    type = "string";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return type;
    }

    public static DBTypeEnum getDbType(Connection connection) {

        String dirverName;
        try {
            dirverName = connection.getMetaData().getDriverName().toLowerCase();
        } catch (SQLException e) {
            return DBTypeEnum.Generic;
        }
        //sql server，不使用url的原因是避免连接中带有相应关键字
        dirverName = dirverName.replace(" ", "");

        DBTypeEnum type = DBTypeEnum.Generic;

        if (dirverName.toLowerCase().contains(DBTypeEnum.Oracle.name().toLowerCase())) {
            type = DBTypeEnum.Oracle;
        } else if (dirverName.toLowerCase().contains(DBTypeEnum.Mysql.name().toLowerCase())) {
            type = DBTypeEnum.Mysql;
        } else if (dirverName.toLowerCase().contains("mariadb")) {
            type = DBTypeEnum.Mysql;
        } else if (dirverName.toLowerCase().contains(DBTypeEnum.PostgreSQL.name().toLowerCase())) {
            type = DBTypeEnum.PostgreSQL;
        } else if (dirverName.toLowerCase().contains(DBTypeEnum.SQLServer.name().toLowerCase())) {
            type = DBTypeEnum.SQLServer;
        } else if (dirverName.toLowerCase().contains(DBTypeEnum.Greenplum.name().toLowerCase())) {
            type = DBTypeEnum.Greenplum;
        } else if (dirverName.toLowerCase().contains(DBTypeEnum.DB2.name().toLowerCase())) {
            type = DBTypeEnum.DB2;
        } else if (dirverName.toLowerCase().contains(DBTypeEnum.OSCAR.name().toLowerCase())) {
            type = DBTypeEnum.OSCAR;
        }else if (dirverName.toLowerCase().contains(DBTypeEnum.DM.name().toLowerCase())) {
            type = DBTypeEnum.DM;
        }

        return type;

    }

    private static boolean isOracle(DatabaseMetaData dmd) {

        String dirverName;
        try {
            dirverName = dmd.getDriverName().toLowerCase();
        } catch (SQLException e) {

            return false;
        }

        return dirverName.contains("oracle");

    }

    private static boolean isMysql(DatabaseMetaData dmd) {

        String url;
        try {
            url = dmd.getURL().toLowerCase();
        } catch (SQLException e) {

            return false;
        }

        return url.contains("mysql");

    }

    private static boolean isPostgresql(DatabaseMetaData dmd) {

        String url;
        try {
            url = dmd.getURL().toLowerCase();
        } catch (SQLException e) {
            return false;
        }

        return url.contains("postgresql");

    }


    public Connection getConn() {
        return conn;
    }

    public void setConn(Connection conn) {
        this.conn = conn;
    }

    public PreparedStatement getPst() {
        return pst;
    }

    public void setPst(PreparedStatement pst) {
        this.pst = pst;
    }

    public Statement getSt() {
        return st;
    }

    public void setSt(Statement st) {
        this.st = st;
    }

    public ResultSet getRs() {
        return rs;
    }

    public void setRs(ResultSet rs) {
        this.rs = rs;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void executeQuery(String sql) throws SQLException {
        System.out.println(sql);
        st = conn.createStatement();
        rs = st.executeQuery(sql);
    }

    public void executeUpdate(String sql) throws SQLException {
        System.out.println(sql);
        st = conn.createStatement();//准备执行语句
        st.executeUpdate(sql);
    }

    public void close() {
        try {
            if (null != this.rs) {
                this.rs.close();
            }
            if (null != this.pst) {
                this.pst.close();
            }
            if (null != this.st) {
                this.st.close();
            }
            if (null != this.conn) {
                this.conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<String> getTableNames(String schema, String tableType) {
        List<String> tableNames = new ArrayList<>();

        try {
            //获取数据库的元数据
            DatabaseMetaData dbmd = conn.getMetaData();
            //mysql低版本驱动及greenplum驱动不支持getSchema()方法
            if (StringUtils.isBlank(schema) && !DBTypeEnum.Mysql.value().equals(getDbType(conn).value()) && !DBTypeEnum.Greenplum.value().equals(getDbType(conn).value())) {
                schema = conn.getSchema();
            }
            if(StringUtils.isBlank(tableType)){
                tableType = TableType.TABLE.toString();
            }
            //从元数据中获取到所有的表名
            rs = dbmd.getTables(conn.getCatalog(), schema, null, new String[]{tableType});
            while (rs.next()) {
                tableNames.add(rs.getString(3));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            this.close();
        }
        return tableNames;
    }

    //根据数据源Id查询数据表名称跟列名
    public Map<String,List<String>> getTableAndColumnsByDatasourceId(String schema, String tableType) {
        Map<String,List<String>> tableNameAndColumns = new LinkedHashMap<>();
        try {
            //获取数据库的元数据
            DatabaseMetaData dbmd = conn.getMetaData();
            //mysql低版本驱动及greenplum驱动不支持getSchema()方法
            if (StringUtils.isBlank(schema) && !DBTypeEnum.Mysql.value().equals(getDbType(conn).value()) && !DBTypeEnum.Greenplum.value().equals(getDbType(conn).value())) {
                schema = conn.getSchema();
            }
            if(StringUtils.isBlank(tableType)){
                tableType = TableType.TABLE.toString();
            }
            //从元数据中获取到所有的表名
            rs = dbmd.getTables(conn.getCatalog(), schema, null, new String[]{tableType});
            while (rs.next()) {
                String tableName = rs.getString(3);
                ResultSet rsColumn = dbmd.getColumns(conn.getCatalog(), schema, tableName, null);
                List<String> list = new ArrayList<String>();
                while (rsColumn.next()) {
                    String colName = rsColumn.getString("COLUMN_NAME");
                    list.add(colName);
                }
                tableNameAndColumns.put(tableName,list);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            this.close();
        }
        return tableNameAndColumns;
    }

    /**
     * 查询指定的表中的所有的字段名列表
     *
     * @param table 表名
     * @return 字段列表["colunm1","colunm2"]
     * @see DBHelper#getTableColumnsInfo(String, String)
     */
    public List<String> getTableColumns(String schema, String table) {
        List<String> list = new ArrayList<String>();
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            //mysql低版本驱动及greenplum驱动不支持getSchema()方法
            if (StringUtils.isBlank(schema) && !DBTypeEnum.Mysql.value().equals(getDbType(conn).value()) && !DBTypeEnum.Greenplum.value().equals(getDbType(conn).value())) {
                schema = conn.getSchema();
            }
            rs = dbmd.getColumns(conn.getCatalog(), schema, table, null);
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                list.add(colName);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            this.close();
        }
        return list;
    }

    /**
     * 查询指定的表信息
     *
     * @param table 表名
     * @return 表信息，包含字段名，字段类型名和字段长度,例如：。[{"colunmSize":"100","colunmType":"VARCHAR","colunmName":"id"},{"colunmSize":"100","colunmType":"VARCHAR",
     * "colunmName":"cluster_id"}]
     * @see DBHelper#getTableColumns(String, String)
     */
    public List<Map<String, String>> getTableColumnsInfo(String schema, String table) throws SQLException {
        List<Map<String, String>> colunms = new ArrayList<>();
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            //mysql低版本驱动及greenplum驱动不支持getSchema()方法
            if (StringUtils.isBlank(schema) && !DBTypeEnum.Mysql.value().equals(getDbType(conn).value()) && !DBTypeEnum.Greenplum.value().equals(getDbType(conn).value())) {
                schema = conn.getSchema();
            }

            rs = dbmd.getColumns(conn.getCatalog(), schema, table, null);
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                String colunmType = rs.getString("TYPE_NAME");
                String colunmSize = rs.getString("COLUMN_SIZE");
                String isNullable = rs.getString("IS_NULLABLE");
                String remarks = rs.getString("REMARKS");
                String isAutoIncrement = null;
                if(!StringUtils.equalsAnyIgnoreCase(getDbType(conn).value(),
                        DBTypeEnum.OSCAR.value(),DBTypeEnum.DM.value())) {
                    isAutoIncrement = rs.getString("IS_AUTOINCREMENT");
                }

                Map<String, String> map = new HashMap<>();
                //列名
                map.put("colunmName", colName);
                //字段类型
                map.put("colunmType", colunmType);
                //字段长度
                map.put("colunmSize", colunmSize);
                //不否可空,true表示可空,false表示非空
                map.put("isNullable", isNullable);
                //字段注释
                map.put("remarks", remarks);
                //是否自增序列
                map.put("isAutoIncrement", isAutoIncrement);

                colunms.add(map);
            }

            List<String> primaryKeys = getPrimaryKey(schema, table);

            //判断是否可空
            colunms.forEach(map -> {

                if (primaryKeys.contains(map.get("colunmName"))) {
                    map.put("isPrimaryKey", "YES");
                } else {
                    map.put("isPrimaryKey", "NO");
                }
            });

        } catch (SQLException e) {
            e.printStackTrace();
            throw new SQLException(e);
        } finally {
            this.close();
        }


        return colunms;
    }

    public List<Map<String, String>> getTableColumnsInfoforEach(String schema, String table) {
        List<Map<String, String>> list = new ArrayList<>();
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            //mysql低版本驱动及greenplum驱动不支持getSchema()方法
            if (StringUtils.isBlank(schema) && !DBTypeEnum.Mysql.value().equals(getDbType(conn).value()) && !DBTypeEnum.Greenplum.value().equals(getDbType(conn).value())) {
                schema = conn.getSchema();
            }
            rs = dbmd.getColumns(conn.getCatalog(), schema, table, null);
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                String colunmType = rs.getString("TYPE_NAME");
                String colunmSize = rs.getString("COLUMN_SIZE");

                Map<String, String> map = new HashMap<>();
                map.put("colunmName", colName);
                map.put("colunmType", colunmType);
                map.put("colunmSize", colunmSize);
                list.add(map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public Map<String, String> getHiveCloumns(String table) throws SQLException {
        Map<String, String> map = new LinkedHashMap<String, String>();
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getColumns(null, null, table, null);
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
//            int type = rs.getInt("DATA_TYPE");
//            map.put(colName,converthive(type));
                map.put(colName, "string");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.close();
        }
        return map;
    }

    public List<String> getPrimaryKey(String schema, String tableName) throws SQLException {

        List<String> pKeys = new ArrayList<>();
        try {
            final DatabaseMetaData dmd = conn.getMetaData();

            if (StringUtils.isBlank(schema) && !DBTypeEnum.Mysql.value().equals(getDbType(conn).value()) && !DBTypeEnum.Greenplum.value().equals(getDbType(conn).value())) {
                schema = conn.getSchema();
            }
            //oracle的schema,就是库名,catalog通常为空;
            //而mysql的schema未使用,catalog为空库名,参见 mysql的驱动~com.mysql.jdbc.DatabaseMetaDataUsingInfoSchema.getPrimaryKeys(如果使用useInformationSchema=true参数)
            //或者 com.mysql.jdbc.DatabaseMetaData.getPrimaryKeys(如果不使用useInformationSchema=true参数))
            //https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-configuration-properties.html
            try (final ResultSet pkrs = dmd.getPrimaryKeys(conn.getCatalog(), schema, tableName)) {

                while (pkrs.next()) {
                    pKeys.add(pkrs.getString("COLUMN_NAME"));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return pKeys;


    }
}
