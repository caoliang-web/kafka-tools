package com.flywheels.doris;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;


public class GeneralJsonUtil {
    public static void main(String[] args) {
        String tableStruct = "CREATE TABLE `ods_athena_system_service_t_delivery_addr_delta` (\n" +
                "  `id` varchar(32) NOT NULL COMMENT '主键',\n" +
                "  `corp_code` varchar(32) DEFAULT NULL COMMENT '租户编码',\n" +
                "  `customer_code` varchar(32) DEFAULT NULL COMMENT '客户编码',\n" +
                "  `customer_type` tinyint(4) DEFAULT NULL COMMENT '客户类型 1集团  2公司 3门店  4档口',\n" +
                "  `is_default` tinyint(4) DEFAULT NULL COMMENT '是否默认 1是 0否',\n" +
                "  `contact_name` varchar(100) DEFAULT NULL COMMENT '联系人名称',\n" +
                "  `contact_phone` varchar(32) DEFAULT NULL COMMENT '联系人联系方式',\n" +
                "  `province` varchar(100) DEFAULT NULL COMMENT '配送地址-省',\n" +
                "  `province_name` varchar(100) DEFAULT NULL COMMENT '配送地址-省名称',\n" +
                "  `city` varchar(100) DEFAULT NULL COMMENT '配送地址-市',\n" +
                "  `city_name` varchar(100) DEFAULT NULL COMMENT '配送地址-市名称',\n" +
                "  `distrct` varchar(100) DEFAULT NULL COMMENT '配送地址-区县',\n" +
                "  `distrct_name` varchar(100) DEFAULT NULL COMMENT '配送地址-区县名称',\n" +
                "  `address` varchar(1000) DEFAULT NULL COMMENT '详细配送地址',\n" +
                "  `longitude` varchar(32) DEFAULT NULL COMMENT '配送地址经度',\n" +
                "  `latitude` varchar(32) DEFAULT NULL COMMENT '配送地址纬度',\n" +
                "  `is_deleted` tinyint(4) DEFAULT NULL COMMENT '是否删除 1删除 0未删除',\n" +
                "  `creator` varchar(100) DEFAULT NULL COMMENT '创建人',\n" +
                "  `create_time` datetime DEFAULT NULL COMMENT '创建时间',\n" +
                "  `last_modifier` varchar(100) DEFAULT NULL COMMENT '最后修改人',\n" +
                "  `last_modify_time` datetime DEFAULT NULL COMMENT '最后修改时间',\n" +
                "  `_is_delete` int(11) DEFAULT '1' COMMENT '0表示删除，1表示正常',\n" +
                "  `updateStamp` datetime\n" +
                ") ";

        String[] split = tableStruct.split("`", -1);
        List<Object> tableAndColList = new ArrayList<>();
        for (String tableAndCol : split) {
            if (!tableAndCol.contains("CREATE") && !tableAndCol.contains("\n")) {
                tableAndColList.add(tableAndCol);
            }
        }
        JSONObject generalJson = new JSONObject();
        StringBuilder sb = new StringBuilder();
        sb.append("[\"$.");
        generalJson.put("doris_table", tableAndColList.get(0));
        tableAndColList.remove(0);

        generalJson.put("max_filter_ratio", "1.0");
        generalJson.put("columns", StringUtils.join(tableAndColList, ","));
        generalJson.put("format", "json");

        String jsonpaths = StringUtils.join(tableAndColList, "\",\"$.");
        sb.append(jsonpaths);
        sb.append("\"]");
        generalJson.put("jsonpaths", sb.toString());
        generalJson.put("strip_outer_array", "true");
        generalJson.put("table_comments", "");


        System.out.println(JSON.toJSONString(generalJson, SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue,
                SerializerFeature.WriteDateUseDateFormat));
    }
}
