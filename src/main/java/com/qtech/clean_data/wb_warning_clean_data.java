package com.qtech.clean_data;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class wb_warning_clean_data {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";      //指定kudu地址

//        kuduMaster = "dev02";
        kuduTable = "WB_WARNING_CLEAN_DATA";         //指定表名

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
                            "WB_WARNING_CLEAN_DATA_BAK"));
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void alterColumn() {

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
                    "UPDATE_TIME", Type.STRING));
        } catch (KuduException e) {
            e.printStackTrace();
        }



        //删除字段，但是主键不能删除

//        try {
//            kuduClient.alterTable(kuduTable,new AlterTableOptions().dropColumn("part_spec"));
//        } catch (KuduException e) {
//            e.printStackTrace();
//        }
//
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

                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("FACTORY_NAME", Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WORKSHOP_CODE", Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MACHINE_NO", Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DEVICE_M_ID", Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("FISCAL_DATE", Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TIME_PERIOD", Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PART_SPEC", Type.STRING).key(true).build());


                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SHIFT_ID", Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OUTPUT", Type.INT32).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WARN_COUNT", Type.INT32).nullable(true).build());

                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PRODUCTION_TIME",
                            Type.FLOAT).nullable(true).build());

                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DOWNTIME", Type.FLOAT).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder(
                            "TOTAL_IDLE_TIME", Type.FLOAT).nullable(true).build());


                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("UPH_IDEAL", Type.FLOAT).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("UPH_NET", Type.FLOAT).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MTBA", Type.FLOAT).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MTBF", Type.FLOAT).nullable(true).build());

                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NUMBER_OF_1ST__NON_STICK_ERROR", Type.FLOAT).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NUMBER_OF_2ND__NON_STICK_ERROR", Type.FLOAT).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NUMBER_OF_SHORT_OF_TAIL", Type.FLOAT).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NUMBER_OF_BALL_MISSING", Type.FLOAT).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NUMBER_OF_LEAD_QUALITY_FAIL", Type.FLOAT).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NUMBER_OF_DIE_QUALITY_FAIL", Type.FLOAT).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NUMBER_OF_EJECTOR_ERROR", Type.FLOAT).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NUMBER_OF_INDEX", Type.FLOAT).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NUMBER_OF_WINDOW_CLAMP_ERROR", Type.FLOAT).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NUMBER_OF_OUTPUT_MAGAZINE_FAIL", Type.FLOAT).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NUMBER_OF_LEAD_TOLERANCE_FAIL", Type.FLOAT).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NUMBER_OF_DIE_TOLERANCE_FAIL", Type.FLOAT).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NUMBER_OF_LOCAL_LEAD_FAIL", Type.FLOAT).nullable(true).build());


                    Schema schema = new Schema(columnSchemas);

                    //指定创建表的相关属性
                    CreateTableOptions Options = new CreateTableOptions();

                    // 设置表的replica备份和分区规则
                    ArrayList<String> partitionList = new ArrayList<String>();

                    //根据字段id分区
                    partitionList.add("DEVICE_M_ID");

                    //设置表的Hash分区
                    Options.addHashPartitions(partitionList, 3);

                    //设置表的备份数
                    Options.setNumReplicas(3);

                    //设置range分区
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