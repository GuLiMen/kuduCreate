package com.qtech.cs;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class ads_cs_list {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                                    //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";       //指定kudu地址
//        kuduMaster = "dev02";
        kuduTable = "ADS_CS_LIST";                          //指定表名

        //创建连接
        KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMaster);
        kuduClientBuilder.defaultAdminOperationTimeoutMs(6000);
        kuduClient = kuduClientBuilder.build();
    }

    @After
    public void close(){
        if (kuduClient != null){
            //关闭连接
            try {
                kuduClient.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void renameTable(){
        try {
            kuduClient.alterTable(kuduTable,
                    new AlterTableOptions().renameTable("BI_QUALITY_KEY_MATERIAL_TYPE"));
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    @Test
    /**
     * 字段
     */
    public void alterColumn(){

        //创建非空字段
//        try {
//            int l = 0;
//            kuduClient.alterTable(kuduTable,new AlterTableOptions().addColumn(
//                    "des_id",Type.INT32,l));
//        } catch (KuduException e) {
//            e.printStackTrace();
//        }

        //创建默认为空字段
        try {
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn(
                    "OwnerName", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn(
                    "OwnerDept", Type.STRING));
        } catch (KuduException e) {
            e.printStackTrace();
        }


        //删除字段，但是主键不能删除

//        try {
//            kuduClient.alterTable(kuduTable,new AlterTableOptions().dropColumn("part_spec"));
//        } catch (KuduException e) {
//            e.printStackTrace();
//        }

    }

    /**
     * kudu创建表
     */
    @Test
    public void creatTable(){

        try {

            //判断表是否存在
            if (!kuduClient.tableExists(kuduTable)){

                //设置表的schema
                ArrayList<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("uuid", Type.STRING).key(true).comment("序号").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("id",Type.STRING).nullable(true).comment("序号").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("csdate",Type.STRING).nullable(true).comment("投诉时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("sampleacceptdate",Type.STRING).nullable(true).comment("不良接收时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("customername",Type.STRING).nullable(true).comment("客户名称").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("qtmodel",Type.STRING).nullable(true).comment("QT机种").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("modelstatus",Type.STRING).nullable(true).comment("机型状态").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("modeltype",Type.STRING).nullable(true).comment("模组类型").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("csstage",Type.STRING).nullable(true).comment("投诉站别").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("customerng",Type.STRING).nullable(true).comment("退回数量").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("csphenomenon",Type.STRING).nullable(true).comment("不良现象").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("customerngrate",Type.STRING).nullable(true).comment("不良率").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("responsibledepartment",Type.STRING).nullable(true).comment("责任单位").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("samplestatus",Type.STRING).nullable(true).comment("不良状态").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ngcode",Type.STRING).nullable(true).comment("不良喷码").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("lastresult",Type.STRING).nullable(true).comment("不良原因").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("attr2",Type.STRING).nullable(true).comment("改善措施").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("assignusername",Type.STRING).nullable(true).comment("责任人").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("completionstatus",Type.STRING).nullable(true).comment("完成状况").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("active",Type.STRING).nullable(true).comment("结案与否").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("startdate",Type.STRING).nullable(true).comment("开始时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("enddate",Type.STRING).nullable(true).comment("结束时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("realdate",Type.STRING).nullable(true).comment("完成时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("role",Type.STRING).nullable(true).comment("角色").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("status",Type.STRING).nullable(true).comment("状态").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("comment",Type.STRING).nullable(true).comment("备注").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("gbissue",Type.STRING).nullable(true).comment("高爆不良").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("executeissue",Type.STRING).nullable(true).comment("执行力不良").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("tbuser",Type.STRING).nullable(true).comment("提报人").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("createdate",Type.STRING).nullable(true).comment("创建时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("customermodel",Type.STRING).nullable(true).comment("客户机型").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("customerinput",Type.STRING).nullable(true).comment("客户投入总数").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ksgrade",Type.STRING).nullable(true).comment("客诉等级").build());

                Schema schema = new Schema(columnSchemas);

                //指定创建表的相关属性
                CreateTableOptions Options = new CreateTableOptions();

                // 设置表的replica备份和分区规则
                ArrayList<String> partitionList = new ArrayList<String>();

                //根据字段id分区
                partitionList.add("uuid");
                //设置表的Hash分区
                Options.addHashPartitions(partitionList,3);

                //设置表的备份数
                Options.setNumReplicas(3);

                //设置range分区
                Options.setRangePartitionColumns(partitionList);

                //创建表
                kuduClient.createTable(kuduTable, schema, Options);
            }
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }
}
