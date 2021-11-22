package com.qtech.ads;

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

public class dws_aa_is_nwtwork {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";      //指定kudu地址

//        kuduMaster = "dev02";
        kuduTable = "DWS_AA_IS_NWTWORK_TEST";         //指定表名

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
    public void renameTable(){
        try {
            kuduClient.alterTable(kuduTable,
                    new AlterTableOptions().renameTable("DWS_AA_IS_NWTWORK"));
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }


    @Test
    /**
     * 字段
     */
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

//        Date dNow = new Date( );
//        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
        try {

//            kuduClient.alterTable(kuduTable, new AlterTableOptions().dropColumn("PERIOD"));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn(
                    "LOT", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn(
                    "REMARKS", Type.STRING));
//            kuduClient.alterTable(kuduTable, new AlterTableOptions().dropColumn("MAJOR_CATEGORIES"));

//            kuduClient.alterTable(kuduTable, new ReplicaSelection(kuduTable));
        } catch (KuduException e) {
            e.printStackTrace();
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

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("AREA", Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("COB", Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("EQID", Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DEVICE_NUMBER", Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TIMESLOT", Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("UPLOADTIME", Type.STRING).nullable(true).build());

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("IS_NETWORK", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("IS_PRODUCTION", Type.STRING).nullable(true).build());

                Schema schema = new Schema(columnSchemas);

                //指定创建表的相关属性
                CreateTableOptions Options = new CreateTableOptions();

                // 设置表的replica备份和分区规则
                ArrayList<String> partitionList = new ArrayList<String>();

                //根据字段id分区
                partitionList.add("EQID");

                //设置表的Hash分区
                Options.addHashPartitions(partitionList, 8);

                //设置表的备份数
                Options.setNumReplicas(3);

//                //设置range分区
////                Options.setRangePartitionColumns(partitionList);
//                String[] keys = new String[]{"2020-10-01", "2020-10-15", "2020-11-01", "2020-11-15", "2020-12-01", "2020-12-15", "2021-01-01", "2021-01-15", "2021-02-01", "2021-02-15", "2021-03-01", "2021-03-15", "2021-04-01", "2021-04-15", "2021-05-01"};
//                for (String key : keys) {
//                    PartialRow partialRow = schema.newPartialRow();
//                    partialRow.addString("FISCAL_DATE", key);
//                    Options.addSplitRow(partialRow);
//                }
//                ArrayList<String> partitionList1 = new ArrayList<String>();
//
//                partitionList1.add("FISCAL_DATE");
////                Options.setRangePartitionColumns(Collections.singletonList("TESTTIME"));
//                Options.setRangePartitionColumns(partitionList1);

                //创建表
                kuduClient.createTable(kuduTable, schema, Options);
            }
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }
}
