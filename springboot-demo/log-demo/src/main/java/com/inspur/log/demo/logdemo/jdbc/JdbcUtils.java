package com.inspur.log.demo.logdemo.jdbc;

import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

/**
 * @Auther: wangqingming
 * @Date: 2018/11/6 09:41
 * @Description:
 */
public class JdbcUtils {


    private static String oracleDb_Driver = null;
    private static String oracleDb_Url = null;
    private static String oracleDb_UserName = null;
    private static String oracleDb_Password = null;

    static{
        try{
            //读取db.properties文件中的数据库连接信息
            InputStream in = JdbcUtils.class.getClassLoader().getResourceAsStream("db.properties");
            Properties prop = new Properties();
            prop.load(in);

            //获取数据库连接驱动
            oracleDb_Driver = prop.getProperty("oracleDb_Driver");
            //获取数据库连接URL地址
            oracleDb_Url = prop.getProperty("oracleDb_Url");
            //获取数据库连接用户名
            oracleDb_UserName = prop.getProperty("oracleDb_UserName");
            //获取数据库连接密码
            oracleDb_Password = prop.getProperty("oracleDb_Password");

            //加载数据库驱动
            Class.forName(oracleDb_Driver);

        }catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * @Method: getOracleConnection
     * @Description: 获取Oracle数据库连接对象
     * @Anthor:孤傲苍狼
     *
     * @return Connection数据库连接对象
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException{
        return DriverManager.getConnection(oracleDb_Url, oracleDb_UserName,oracleDb_Password);
    }

    /**
     * @Method: release
     * @Description: 释放资源，
     *     要释放的资源包括Connection数据库连接对象，负责执行SQL命令的Statement对象，存储查询结果的ResultSet对象
     * @Anthor:孤傲苍狼
     *
     * @param conn
     * @param st
     * @param rs
     */
    public static void release(Connection conn,Statement st,ResultSet rs){
        if(rs!=null){
            try{
                //关闭存储查询结果的ResultSet对象
                rs.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
            rs = null;
        }
        if(st!=null){
            try{
                //关闭负责执行SQL命令的Statement对象
                st.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

        if(conn!=null){
            try{
                //关闭Connection数据库连接对象
                conn.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
