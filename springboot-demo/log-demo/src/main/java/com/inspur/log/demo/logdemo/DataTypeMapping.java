package com.inspur.log.demo.logdemo;

import com.inspur.log.demo.logdemo.jdbc.JdbcUtils;
import net.sf.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataTypeMapping {

    public static void main(String[] args) {
        String file = DataTypeMapping.class.getClassLoader().getResource("datatypemapping.json").getFile();

        String substring = file.substring(1);

        String s1 = null;
        try {
            s1 = new String(Files.readAllBytes(Paths.get(substring)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        parseJson(s1);
    }


    public static void parseJson(String content) {
        try {
            JSONObject jsonObj = JSONObject.fromObject(content);

            JSONObject dbToStandard = jsonObj.getJSONObject("dbToStandard").getJSONObject("newSql".toLowerCase());  // 来源库类型sourceDbType对应的数据库字段类型映射
            JSONObject standardToDb = jsonObj.getJSONObject("standardToDb").getJSONObject("oracle".toLowerCase());  // 目标库类型targetDbType对应的数据库字段类型映射

            String standardColunmType = dbToStandard.getString("bool".toLowerCase()); // 源表的当前字段类型对应的标准字段类型
        } catch (Exception e) {
            String errorMsg = ExceptionUtils.getStackTrace(e);
            System.out.printf(errorMsg);
        }


    }


    public static List<Map<String, String>> getTableColumnInfo(String catalog, String schema, String tableName) throws SQLException {
        List<Map<String, String>> result = new ArrayList<Map<String, String>>();

        try (Connection connection = JdbcUtils.getConnection();) {

            ResultSet rs = null;

            DatabaseMetaData dbmd = connection.getMetaData();
            rs = dbmd.getColumns(catalog, schema, tableName, null);
            while (rs.next()) {
                Map<String, String> resultMap = new HashMap<String, String>();
                resultMap.put("TABLE_CAT", rs.getString("TABLE_CAT"));
                // 表模式（可为 null）
                resultMap.put("TABLE_SCHEM", rs.getString("TABLE_SCHEM"));
                // 表名称
                resultMap.put("TABLE_NAME", rs.getString("TABLE_NAME"));
                // 列名称
                resultMap.put("COLUMN_NAME", rs.getString("COLUMN_NAME"));

                //字段类型
                resultMap.put("DATA_TYPE", rs.getString("DATA_TYPE"));
                // 数据源依赖的类型名称
                resultMap.put("TYPE_NAME", rs.getString("TYPE_NAME"));
                // 列的大小
                resultMap.put("COLUMN_SIZE", rs.getString("COLUMN_SIZE"));
                // 小数精度
                resultMap.put("DECIMAL_DIGITS", rs.getString("DECIMAL_DIGITS"));
                // 是否允许使用 NULL(0:否;1:是)
                resultMap.put("NULLABLE", rs.getString("NULLABLE"));
                // 默认值
                String columnDef = rs.getString("COLUMN_DEF");
                resultMap.put("COLUMN_DEF", StringUtils.isNotEmpty(columnDef) ? columnDef : null);
                // 描述列的注释（可为 null）
                String remarks = rs.getString("REMARKS");
                resultMap.put("REMARKS", StringUtils.isNotEmpty(remarks) ? remarks : null);
                result.add(resultMap);
            }
        }

        return result;
        /**
         * 每个列描述都有以下列：
         *
         * TABLE_CAT String => 表类别（可为 null）
         * TABLE_SCHEM String => 表模式（可为 null）
         * TABLE_NAME String => 表名称
         * COLUMN_NAME String => 列名称
         * DATA_TYPE int => 来自 java.sql.Types 的 SQL 类型
         * TYPE_NAME String => 数据源依赖的类型名称，对于 UDT，该类型名称是完全限定的
         * COLUMN_SIZE int => 列的大小。
         * BUFFER_LENGTH 未被使用。
         * DECIMAL_DIGITS int => 小数部分的位数。对于 DECIMAL_DIGITS 不适用的数据类型，则返回 Null。
         * NUM_PREC_RADIX int => 基数（通常为 10 或 2）
         * NULLABLE int => 是否允许使用 NULL。
         * columnNoNulls - 可能不允许使用 NULL 值
         * columnNullable - 明确允许使用 NULL 值
         * columnNullableUnknown - 不知道是否可使用 null
         * REMARKS String => 描述列的注释（可为 null）
         * COLUMN_DEF String => 该列的默认值，当值在单引号内时应被解释为一个字符串（可为 null）
         * SQL_DATA_TYPE int => 未使用
         * SQL_DATETIME_SUB int => 未使用
         * CHAR_OCTET_LENGTH int => 对于 char 类型，该长度是列中的最大字节数
         * ORDINAL_POSITION int => 表中的列的索引（从 1 开始）
         * IS_NULLABLE String => ISO 规则用于确定列是否包括 null。
         * YES --- 如果参数可以包括 NULL
         * NO --- 如果参数不可以包括 NULL
         * 空字符串 --- 如果不知道参数是否可以包括 null
         * SCOPE_CATLOG String => 表的类别，它是引用属性的作用域（如果 DATA_TYPE 不是 REF，则为 null）
         * SCOPE_SCHEMA String => 表的模式，它是引用属性的作用域（如果 DATA_TYPE 不是 REF，则为 null）
         * SCOPE_TABLE String => 表名称，它是引用属性的作用域（如果 DATA_TYPE 不是 REF，则为 null）
         * SOURCE_DATA_TYPE short => 不同类型或用户生成 Ref 类型、来自 java.sql.Types 的 SQL 类型的源类型（如果 DATA_TYPE 不是 DISTINCT 或用户生成的 REF，则为 null）
         * IS_AUTOINCREMENT String => 指示此列是否自动增加
         * YES --- 如果该列自动增加
         * NO --- 如果该列不自动增加
         * 空字符串 --- 如果不能确定该列是否是自动增加参数
         * COLUMN_SIZE 列表示给定列的指定列大小。对于数值数据，这是最大精度。对于字符数据，这是字符长度。对于日期时间数据类型，这是 String 表示形式的字符长度（假定允许的最大小数秒组件的精度）。对于二进制数据，这是字节长度。对于 ROWID 数据类型，这是字节长度。对于列大小不适用的数据类型，则返回 Null。
         *
         *
         * 参数：
         * catalog - 类别名称；它必须与存储在数据库中的类别名称匹配；该参数为 "" 表示获取没有类别的那些描述；为 null 则表示该类别名称不应该用于缩小搜索范围
         * schemaPattern - 模式名称的模式；它必须与存储在数据库中的模式名称匹配；该参数为 "" 表示获取没有模式的那些描述；为 null 则表示该模式名称不应该用于缩小搜索范围
         * tableNamePattern - 表名称模式；它必须与存储在数据库中的表名称匹配
         * columnNamePattern - 列名称模式；它必须与存储在数据库中的列名称匹配
         */
    }


}
