package com.flywheels.doris.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);


    public static int[] executeBatch(String hostUrl,String db,String user,String password,String[] sqls) throws SQLException{
        String connectionUrl = String.format("jdbc:mysql://%s/%s",hostUrl,db);
        Connection con = null;
        try {
            con = DriverManager.getConnection(connectionUrl,user,password);
            Statement statement = con.createStatement();
            for(String sql : sqls){
                statement.addBatch(sql);
            }
            long start = System.currentTimeMillis();
            int[] ints = statement.executeBatch();
            long end = System.currentTimeMillis();
            LOG.info("SQL执行耗时:{}ms,执行结果:{}" ,(end-start), JSON.toJSONString(ints));
            statement.close();
            return ints;
        } catch (SQLException e) {
            LOG.error("SQL执行异常",e);
            throw e;
        }  finally {
            try {
                con.close();
            } catch (Exception e) {
                LOG.warn("连接关闭错误",e);
            }
        }
    }

    public static List<JSONObject> executeQuery(String hostUrl,String db,String user,String password,String sql){
        List<JSONObject> beJson = new ArrayList<>();
        String connectionUrl = String.format("jdbc:mysql://%s/%s",hostUrl,db);
        Connection con = null;
        try {
            con = DriverManager.getConnection(connectionUrl,user,password);
            PreparedStatement ps = con.prepareStatement(sql);
            long start = System.currentTimeMillis();
            ResultSet rs = ps.executeQuery();
            long end = System.currentTimeMillis();
            LOG.info("SQL执行耗时:ms:" + (end-start));
            beJson = resultSetToJson(rs);
        } catch (SQLException e) {
            LOG.error("SQL执行异常",e);
        } catch (Exception e) {
            LOG.error("SQL执行错误",e);
        } finally {
            try {
                con.close();
            } catch (Exception e) {
                LOG.warn("连接关闭错误",e);
            }
        }
        return beJson;
    }


    /**
     * resultSet转List
     * @param rs
     * @return
     * @throws SQLException
     */
    public static List<JSONObject> resultSetToJson(ResultSet rs) throws SQLException
    {
        //定义接收的集合
        List<JSONObject> list = new ArrayList<JSONObject>();
        // 获取列数
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        // 遍历ResultSet中的每条数据
        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();
            // 遍历每一列
            for (int i = 1; i <= columnCount; i++) {
                String columnName =metaData.getColumnLabel(i);
                String value = rs.getString(columnName);
                jsonObj.put(columnName, value);
            }
            list.add(jsonObj);
        }
        return list;
    }
}
