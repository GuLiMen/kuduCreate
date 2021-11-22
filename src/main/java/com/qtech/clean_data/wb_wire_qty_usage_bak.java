package com.qtech.clean_data;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class wb_wire_qty_usage_bak {

    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";      //指定kudu地址

//        kuduMaster = "dev02";
        kuduTable = "WB_WIRE_QTY_USAGE_TEST";         //指定表名

        //创建连接
        KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMaster);
        kuduClientBuilder.defaultAdminOperationTimeoutMs(6000);
        kuduClient = kuduClientBuilder.build();
    }

    @After
    public void close() {
        if (kuduClient != null) {
            //关闭连接
            try {
                kuduClient.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void renameTable() {
        try {
            kuduClient.alterTable(kuduTable,
                    new AlterTableOptions().renameTable(
                            "WB_WARNING_CLEAN_DATA_BAK4"));
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void alterColumn() {

////        创建非空字段
//        try {
//            int l = 0;
//            kuduClient.alterTable(kuduTable, new AlterTableOptions().addColumn(
//                    "des_id", Type.INT32, l));
//        } catch (KuduException e) {
//            e.printStackTrace();
//        }

//        创建默认为空字段
        try {
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn(
                    "UPDATE_TIME", Type.STRING));
        } catch (KuduException e) {
            e.printStackTrace();
        }

        //修改字段名
        try {
            kuduClient.alterTable(kuduTable,new AlterTableOptions().renameColumn("",""));
        } catch (KuduException e) {
            e.printStackTrace();
        }

//        删除字段，但是主键不能删除

//        try {
//            kuduClient.alterTable(kuduTable, new AlterTableOptions().dropColumn("UPDATE_TIME"));
//        } catch (KuduException e) {
//            e.printStackTrace();
//        }
    }
    /**
     * kudu创建表
     */
    @Test
    public void creatTable () {

        try {

            //判断表是否存在
            if (!kuduClient.tableExists(kuduTable)) {

                //设置表的schema
                ArrayList<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DEVICE_M_ID",Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PART_SPEC",Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WIRE_LOAD_TIME",Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("FACTORY_NAME",Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WORKSHOP",Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TIME_PERIOD",Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("FISCAL_DATE",Type.STRING).key(true).build());

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MACHINE_NO",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MIN_DATE",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MAX_DATE",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WIRE_QTY_MAX",Type.FLOAT).nullable(true).build());


                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ACTUAL_WIRE_QTY",Type.FLOAT).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ATUAL_PROD_QTY",Type.FLOAT).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WIRE_SIZE",Type.FLOAT).nullable(true).build());

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DEVICE_TYPE",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("UPDATE_TIME",Type.STRING).nullable(true).build());

                Schema schema = new Schema(columnSchemas);

                //指定创建表的相关属性
                CreateTableOptions Options = new CreateTableOptions();

                // 设置表的replica备份和分区规则
                ArrayList<String> partitionList = new ArrayList<String>();

                //根据字段id分区
                partitionList.add("DEVICE_M_ID");

                //设置表的Hash分区
                Options.addHashPartitions(partitionList, 5);

                //设置表的备份数
                Options.setNumReplicas(3);

//                //设置range分区
//                Options.setRangePartitionColumns(partitionList);
                String[] keys = new String[]{"2020-10-01", "2020-10-15", "2020-11-01", "2020-11-15", "2020-12-01", "2020-12-15", "2021-01-01", "2021-01-15", "2021-02-01", "2021-02-15", "2021-03-01", "2021-03-15", "2021-04-01", "2021-04-15", "2021-05-01"};
                for (String key : keys) {
                    PartialRow partialRow = schema.newPartialRow();
                    partialRow.addString("FISCAL_DATE", key);
                    Options.addSplitRow(partialRow);
                }
                ArrayList<String> partitionList1 = new ArrayList<String>();

                partitionList1.add("FISCAL_DATE");
//                Options.setRangePartitionColumns(Collections.singletonList("TESTTIME"));
                Options.setRangePartitionColumns(partitionList1);

                //创建表
                kuduClient.createTable(kuduTable, schema, Options);
            }
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

}
