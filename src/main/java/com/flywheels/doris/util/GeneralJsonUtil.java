package com.flywheels.doris.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;


public class GeneralJsonUtil {
    public static void main(String[] args) {
        String tableStruct = "CREATE TABLE if not exists routine_load_test.ods_athena_sale_order_t_shsc_order_item_delta (\n" +
                "  `id` bigint(20) NOT NULL,\n" +
                "  `order_code` varchar(20) DEFAULT NULL COMMENT '订单号',\n" +
                "  `corp_code` varchar(64) DEFAULT NULL COMMENT '集团编码',\n" +
                "  `customer_code` varchar(64) DEFAULT NULL COMMENT '客户编码',\n" +
                "  `create_time` datetime DEFAULT NULL,\n" +
                "  `line_no` int(11) DEFAULT NULL COMMENT '行号',\n" +
                "  `product_code` varchar(50) DEFAULT NULL COMMENT '商品编码',\n" +
                "  `product_name` varchar(150) DEFAULT NULL COMMENT '商品名称',\n" +
                "  `product_name_simple` varchar(200) DEFAULT NULL COMMENT '商品名称拼音简称',\n" +
                "  `status` varchar(100) DEFAULT NULL COMMENT '状态',\n" +
                "  `order_num` decimal(12,2) DEFAULT NULL COMMENT '订单数量',\n" +
                "  `ship_num` decimal(12,2) DEFAULT NULL COMMENT '实发数量',\n" +
                "  `receipt_num` decimal(12,2) DEFAULT NULL COMMENT '签收数量',\n" +
                "  `is_free` tinyint(2) DEFAULT '0' COMMENT '是否免费(0-否，1-是)',\n" +
                "  `price` decimal(13,4) DEFAULT NULL COMMENT '单价',\n" +
                "  `franchise_price` decimal(12,4) DEFAULT NULL COMMENT '加盟价',\n" +
                "  `unit_no` varchar(100) DEFAULT NULL COMMENT '单位',\n" +
                "  `unit_name` varchar(100) DEFAULT NULL COMMENT '单位名称',\n" +
                "  `standard` varchar(512) DEFAULT NULL COMMENT '规格',\n" +
                "  `total_amount` decimal(12,4) DEFAULT NULL COMMENT '总价',\n" +
                "  `remark` varchar(500) DEFAULT NULL COMMENT '备注',\n" +
                "  `cancel_reason` varchar(500) DEFAULT NULL COMMENT '取消原因',\n" +
                "  `cancel_remark` varchar(500) DEFAULT NULL COMMENT '取消说明',\n" +
                "  `creator` varchar(100) DEFAULT NULL,\n" +
                "  `last_modifier` varchar(100) DEFAULT NULL,\n" +
                "  `last_modifier_time` datetime DEFAULT NULL,\n" +
                "  `product_type` tinyint(2) DEFAULT NULL COMMENT '1自营 2带仓',\n" +
                "  `product_img` varchar(200) DEFAULT NULL COMMENT '商品主图',\n" +
                "  `gaia_product_code` varchar(64) DEFAULT NULL COMMENT '内部系统商品单号',\n" +
                "  `supplier_type` varchar(64) DEFAULT NULL COMMENT '供应商类型',\n" +
                "  `supplier_code` varchar(64) DEFAULT NULL COMMENT '供应商code',\n" +
                "  `supplier_name` varchar(200) DEFAULT NULL COMMENT '供应商名称',\n" +
                "  `delivery_type` varchar(64) DEFAULT NULL COMMENT '业务类型 KC ZF ZS 直发 直送  库存',\n" +
                "  `sign_person` varchar(50) DEFAULT NULL COMMENT '签收人',\n" +
                "  `cancel_person` varchar(50) DEFAULT NULL COMMENT '取消人',\n" +
                "  `main_order_code` varchar(64) DEFAULT NULL COMMENT '主订单号',\n" +
                "  `main_line_no` int(11) DEFAULT NULL COMMENT '主订单行号',\n" +
                "  `cooperate_business_id` varchar(64) DEFAULT NULL COMMENT '合作业务编码',\n" +
                "  `cooperate_business_name` varchar(128) DEFAULT NULL COMMENT '合作业务名称',\n" +
                "  `order_merge` tinyint(2) DEFAULT NULL COMMENT '订单是否被交付合单 1是 0 否',\n" +
                "  `lower_tolerance` varchar(100) DEFAULT NULL COMMENT '允差值下浮动',\n" +
                "  `floating_tolerance` varchar(100) DEFAULT NULL COMMENT '允差值上浮动',\n" +
                "  `tenant_code` int(11) DEFAULT NULL COMMENT '租户编号',\n" +
                "  `instance_code` varchar(100) DEFAULT NULL COMMENT '租户实例编号',\n" +
                "  `batch_no` varchar(100) DEFAULT NULL COMMENT '交付发货批次号',\n" +
                "  `customer_order_id` varchar(64) DEFAULT NULL COMMENT '客户订单号',\n" +
                "  `customer_sku_id` varchar(64) DEFAULT NULL COMMENT '客户物料编码',\n" +
                "  `customer_sku_name` varchar(500) DEFAULT NULL COMMENT '客户物料名称',\n" +
                "  `_is_delete` int(11) DEFAULT '1' COMMENT '0表示删除，1表示正常',\n" +
                "  `updateStamp` datetime \n" +
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
