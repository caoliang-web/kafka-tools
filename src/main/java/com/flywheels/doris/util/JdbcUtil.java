package com.flywheels.doris.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

public class JdbcUtil {

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);
    private static int j = 3;



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

    public static void main(String[] args) throws InterruptedException {
//        LinkedBlockingDeque<Integer> linkedBlockingDeque = new LinkedBlockingDeque<>(1);
//        linkedBlockingDeque.put(1);
//        while(true){
//
//            Thread flushThread = new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    int i = 0;
//                    Integer take = null;
//                    try {
//                        take = linkedBlockingDeque.takeLast();
//                        linkedBlockingDeque.add(i);
//                        System.out.println("线程名称："+ Thread.currentThread().getName() +",take" + take);
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    }
//                    i++;
//                }
//            });
//            flushThread.setDaemon(true);
//            flushThread.start();
//        }


//        Iterator<Integer> iterator4 = linkedBlockingDeque.iterator();
//
//        while (iterator4.hasNext()){
//            System.out.println("Iterator的offerFirst结果：" + iterator4.next());
//        }
    }
}
