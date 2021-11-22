package com.qtech.mesc_report;

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

public class T_SNERRORCODE {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";      //指定kudu地址
//        kuduMaster = "dev02";
        kuduTable = "T_SNERRORCODE";         //指定表名

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
                    new AlterTableOptions().renameTable("MESC_T_SNERRORCODE"));
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
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SCARD",Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TSTIMES",Type.INT32).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("EC_CODE",Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TSTYPE",Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OP_CODE",Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("RCARD",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TCARD",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ECG_CODE",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ECT_CODE",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LOCATION",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MAT_CODE",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WO_CODE",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SERIES_CODE",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ITEM_CODE",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("STA_CODE",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LOT_NO",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("HANDLED",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("EATTRIBUTE1",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("EATTRIBUTE2",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MUSER",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MDATE",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("QTY",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WORKSHOP_CODE",Type.STRING).nullable(true).build());

                Schema schema = new Schema(columnSchemas);

                //指定创建表的相关属性
                CreateTableOptions Options = new CreateTableOptions();

                // 设置表的replica备份和分区规则
                ArrayList<String> partitionList = new ArrayList<String>();

                //根据字段id分区
                partitionList.add("SCARD");
                partitionList.add("TSTIMES");
                partitionList.add("EC_CODE");
                partitionList.add("TSTYPE");
                partitionList.add("OP_CODE");
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