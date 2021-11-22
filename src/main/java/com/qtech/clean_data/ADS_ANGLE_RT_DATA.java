package com.qtech.clean_data;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.PartialRow;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;

public class ADS_ANGLE_RT_DATA {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";      //指定kudu地址
//        kuduMaster = "dev02";
        kuduTable = "ADS_ANGLE_RT_DATA_TEST";         //指定表名

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

//    @Test
//    public void renameTable(){
//        try {
//            kuduClient.alterTable(kuduTable,
//                    new AlterTableOptions().renameTable("BI_MES_WORK_ORDER_OF_DAT"));
//        } catch (KuduException e) {
//            e.printStackTrace();
//        }
//    }

    /**
     *  kudu创建表
     */
    @Test
    public void creatTable(){

        try {

            //判断表是否存在
            if (!kuduClient.tableExists(kuduTable)){

                // 设置表的schema
                ArrayList<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("EQUIPMENTID", Type.STRING).comment("设备码").key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MCID", Type.STRING).comment("机种名").key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DATETIME",Type.STRING).comment("时间").key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LINENMB",Type.INT32).comment("线编号").key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LEADX",Type.STRING).comment("").nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LEADY",Type.STRING).comment("").nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PADX",Type.STRING).comment("").nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PADY",Type.STRING).comment("").nullable(true).build());
                Schema schema = new Schema(columnSchemas);

                // 指定创建表的相关属性
                CreateTableOptions Options = new CreateTableOptions();


                // 设置表的备份数
                Options.setNumReplicas(3);

                // 设置range分区
                String[] keys = new String[]{"2021-10-01","2021-11-01","2021-12-01","2022-01-01"};
                for (String key : keys) {
                    PartialRow partialRow = schema.newPartialRow();
                    partialRow.addString("DATETIME", key);
                    Options.addSplitRow(partialRow);
                }
                ArrayList<String> partitionList1 = new ArrayList<String>();

                partitionList1.add("DATETIME");

                Options.setRangePartitionColumns(partitionList1);

                // 创建表
                kuduClient.createTable(kuduTable, schema, Options);
            }
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

}
