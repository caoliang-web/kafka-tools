package com.flywheels.doris;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;


public class GeneralJsonUtil {
    public static void main(String[] args) {
        String tableStruct = "CREATE TABLE `ods_athena_customer_service_01_t_product_sku_delta` (\n" +
                "  `id` bigint(20) NOT NULL COMMENT '主键id',\n" +
                "  `corp_code` varchar(32) DEFAULT NULL COMMENT '租户编码',\n" +
                "  `customer_code` varchar(32) DEFAULT NULL COMMENT '客户编码',\n" +
                "  `spu_code` varchar(32) NOT NULL COMMENT '商品spuID',\n" +
                "  `spu_type` varchar(8) NOT NULL DEFAULT 'ZSYS' COMMENT '商品类型',\n" +
                "  `sku_code` varchar(32) NOT NULL COMMENT '商品skuID',\n" +
                "  `sku_name` varchar(150) NOT NULL COMMENT '商品sku名称',\n" +
                "  `sku_name_py_full` varchar(256) DEFAULT NULL COMMENT 'sku名称拼音全拼',\n" +
                "  `sku_name_py_short` varchar(64) DEFAULT NULL COMMENT 'sku名称拼音简拼',\n" +
                "  `sku_type` varchar(5) DEFAULT NULL COMMENT '1：自营；2:代仓',\n" +
                "  `category_third_id` varchar(32) NOT NULL COMMENT '三级类目id',\n" +
                "  `unit_name` varchar(120) NOT NULL COMMENT '单位名称',\n" +
                "  `unit_code` varchar(32) NOT NULL COMMENT '单位编码',\n" +
                "  `base_unit_code` varchar(32) NOT NULL COMMENT '基本单位编码',\n" +
                "  `base_unit_name` varchar(100) NOT NULL COMMENT '基本单位名称',\n" +
                "  `sku_extend_properties` varchar(1500)  DEFAULT NULL COMMENT '商品扩展属性',\n" +
                "  `sku_properties` varchar(5000) DEFAULT NULL COMMENT '商品属性',\n" +
                "  `sku_specs` varchar(1000) DEFAULT NULL COMMENT '商品规格',\n" +
                "  `conversion_value` varchar(16) DEFAULT NULL COMMENT '换算比值',\n" +
                "  `conversion_numerator` int(8) DEFAULT NULL COMMENT '转化分子',\n" +
                "  `conversion_denominatr` int(8) DEFAULT NULL COMMENT '转化分母',\n" +
                "  `source_code` varchar(32) DEFAULT NULL COMMENT '源系统编码',\n" +
                "  `sap_code` varchar(32) DEFAULT NULL COMMENT 'sap编码',\n" +
                "  `product_owner_type` int(2) DEFAULT NULL COMMENT '所属集团类型【1代仓客户|2供应商|3加工厂|4蜀海公司】',\n" +
                "  `product_owner_code` varchar(32) DEFAULT NULL COMMENT '所属集团编码',\n" +
                "  `creator` varchar(100) DEFAULT NULL COMMENT '创建人',\n" +
                "  `create_time` datetime DEFAULT NULL COMMENT '创建时间',\n" +
                "  `last_modifier` varchar(100) DEFAULT NULL COMMENT '最后修改人',\n" +
                "  `last_modify_time` datetime DEFAULT NULL COMMENT '最后修改时间',\n" +
                "  `fixed_assets_flag` varchar(16) DEFAULT '0' COMMENT '是否固定资产,0否,1是',\n" +
                "  `check_rule1` varchar(32) DEFAULT NULL COMMENT '费用核算规则-1级科目',\n" +
                "  `check_rule2` varchar(32) DEFAULT NULL COMMENT '费用核算规则-2级科目',\n" +
                "  `standard_desc` varchar(150) DEFAULT NULL COMMENT '规格描述',\n" +
                "  `_is_delete` int(11) DEFAULT '1' COMMENT '0表示删除，1表示正常',\n" +
                "  `updateStamp` datetime \n" +
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
