package com.flywheels.doris;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;


public class GeneralJsonUtil {
    public static void main(String[] args) {
        String tableStruct = "CREATE TABLE `ods_athena_system_service_delta` (\n" +
                "  `id` bigint(32) NOT NULL COMMENT '主键id',\n" +
                "  `user_id` varchar(50) DEFAULT NULL COMMENT '用户id',\n" +
                "  `org_role_id` varchar(50) DEFAULT NULL COMMENT '组织角色id',\n" +
                "  `corp_code` varchar(32) DEFAULT NULL COMMENT '集团编码',\n" +
                "  `customer_code` varchar(32) DEFAULT NULL COMMENT '客户编码',\n" +
                "  `_is_delete` int(11) DEFAULT '1' COMMENT '0表示删除，1表示正常',\n" +
                "  `updateStamp` datetime\n" +
                ")";

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
