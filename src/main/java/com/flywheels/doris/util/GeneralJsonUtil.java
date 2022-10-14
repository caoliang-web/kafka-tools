package com.flywheels.doris.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;


public class GeneralJsonUtil {
    public static void main(String[] args) {
        String tableStruct = "CREATE TABLE `ods_pos_pro_taste_name_delta` (\n" +
                "  `id` bigint NOT NULL COMMENT 'ID',\n" +
                "  `shop_id` varchar(64) DEFAULT NULL,\n" +
                "  `company_id` varchar(64) DEFAULT NULL,\n" +
                "  `relate_id` varchar(64) DEFAULT NULL,\n" +
                "  `taste_type_id` varchar(64) DEFAULT NULL COMMENT '口味类型ID',\n" +
                "  `taste_name` varchar(64) DEFAULT NULL COMMENT '口味名称',\n" +
                "  `enable` char(1) DEFAULT NULL COMMENT '启用状态',\n" +
                "  `disable_time` bigint DEFAULT NULL,\n" +
                "  `disable_time_format` datetime DEFAULT NULL COMMENT '停用时间',\n" +
                "  `sort_no` int(11) DEFAULT NULL COMMENT '排序号',\n" +
                "  `remark` varchar(600) DEFAULT NULL,\n" +
                "  `is_default` char(1) DEFAULT NULL COMMENT '是否默认',\n" +
                "  `dr` char(1) DEFAULT NULL COMMENT '删除状态',\n" +
                "  `ts` bigint DEFAULT NULL,\n" +
                "  `ts_format` datetime DEFAULT NULL COMMENT '下单时间', \n" +
                "  `taste_code` varchar(200) DEFAULT NULL,\n" +
                "  `parent_id` varchar(64) DEFAULT NULL,\n" +
                "  `is_private` char(1) DEFAULT NULL,\n" +
                "  `label_style` char(1) DEFAULT NULL COMMENT '挡位样式  0  单选框 1 拖动条',\n" +
                "  `i18n` varchar(600) DEFAULT NULL,\n" +
                "  `create_time_offset` datetime NULL COMMENT '创建时间'\n" +
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
