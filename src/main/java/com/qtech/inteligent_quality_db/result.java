package com.qtech.inteligent_quality_db;

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

public class result {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";      //指定kudu地址

//        kuduMaster = "dev02";
        kuduTable = "ADS_SPCMESRESULTLIST_TEST2";         //指定表名

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

    /**
     * kudu创建表
     */
    @Test
    public void creatTable() {

        try {

            //判断表是否存在
            if (!kuduClient.tableExists(kuduTable)) {

                //设置表的schema
                ArrayList<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ID", Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TIME", Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("FIRST_TIME", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WORKSHOP_CODE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PART_SPEC", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OP_CODE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("STA_CODE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("EATTRIBUTE1_1", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TESTITEM", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("FACTORY", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ID_NO", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("RCARD", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SAMPLENUM", Type.INT32).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("USL", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LSL", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("AVGUCL", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("AVGLCL", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("RANGEUCL", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("RANGELCL", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("STDDEVUCL", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("STDDEVLCL", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TESTVALUEAVG", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TESTVALUERANGE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TESTVALUESTDEV", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SPCWARN", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SPCGRAPHTYPE", Type.STRING).nullable(true).build());

                Schema schema = new Schema(columnSchemas);
                // 指定创建表的相关属性
                CreateTableOptions Options = new CreateTableOptions();
                // 设置表的备份数
                Options.setNumReplicas(3);

                // 设置range分区
                String[] keys = new String[]{"2020-06-01", "2020-06-15", "2020-07-01", "2020-07-15", "2020-08-01", "2020-08-15", "2020-09-01", "2020-09-15", "2020-10-01"};
                for (String key : keys) {
                    PartialRow partialRow = schema.newPartialRow();
                    partialRow.addString("TIME", key);
                    Options.addSplitRow(partialRow);
                }
                ArrayList<String> partitionList1 = new ArrayList<String>();

                partitionList1.add("TIME");

                Options.setRangePartitionColumns(partitionList1);


                //创建表
                kuduClient.createTable(kuduTable, schema, Options);
            }
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }
}