package com.qtech.BI;

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

public class BI_ZHZL_SUBMIT_AQPAQ_TOTAL {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";      //指定kudu地址
//        kuduMaster = "dev02";
        kuduTable = "BI_ZHZL_SUBMIT_AQPAQ_TOTAL";         //指定表名

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

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ID",Type.INT64).key(true).comment("自增主键").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PART_SPEC",Type.STRING).nullable(true).comment("物料类别").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CUST_NAME",Type.STRING).nullable(true).comment("责任人").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CUST_CODE",Type.STRING).nullable(true).comment("编码规则").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("STAGE",Type.STRING).nullable(true).comment("数据更新时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("START_DATE",Type.STRING).nullable(true).comment("数据更新时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("END_DATE",Type.STRING).nullable(true).comment("数据更新时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("UNIT",Type.STRING).nullable(true).comment("数据更新时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PERSON_DUTY",Type.STRING).nullable(true).comment("数据更新时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("AMOUNT_ACT",Type.STRING).nullable(true).comment("数据更新时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("AMOUNT_UN",Type.STRING).nullable(true).comment("数据更新时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("INSERT_TIME",Type.STRING).nullable(true).comment("数据更新时间").build());


                Schema schema = new Schema(columnSchemas);

                //指定创建表的相关属性
                CreateTableOptions Options = new CreateTableOptions();

                // 设置表的replica备份和分区规则
                ArrayList<String> partitionList = new ArrayList<String>();

                //根据字段id分区
                partitionList.add("ID");
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
