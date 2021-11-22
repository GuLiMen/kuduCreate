package com.qtech.ERP;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class ERP_OOFF_T {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";      //指定kudu地址

//        kuduMaster = "dev02";
        kuduTable = "ERP_OOFF_T";         //指定表名

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

                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFENT",Type.INT64).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFF001",Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFF002",Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFF003",Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFF004",Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFF005",Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFF006",Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFF007",Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFF008",Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFF009",Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFF010",Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFF011",Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFF012",Type.STRING).key(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFSTUS",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFF013",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFF014",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFF015",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFOWNID",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFOWNDP",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFCRTID",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFCRTDP",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFCRTDT",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFMODID",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFMODDT",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD001",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD002",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD003",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD004",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD005",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD006",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD007",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD008",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD009",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD010",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD011",Type.DOUBLE).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD012",Type.DOUBLE).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD013",Type.DOUBLE).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD014",Type.DOUBLE).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD015",Type.DOUBLE).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD016",Type.DOUBLE).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD017",Type.DOUBLE).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD018",Type.DOUBLE).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD019",Type.DOUBLE).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD020",Type.DOUBLE).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD021",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD022",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD023",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD024",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD025",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD026",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD027",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD028",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD029",Type.STRING).nullable(true).build());
                    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OOFFUD030",Type.STRING).nullable(true).build());


                Schema schema = new Schema(columnSchemas);

                //指定创建表的相关属性
                CreateTableOptions Options = new CreateTableOptions();

                // 设置表的replica备份和分区规则
                ArrayList<String> partitionList = new ArrayList<String>();

                //根据字段id分区
                partitionList.add("OOFFENT");
                partitionList.add("OOFF001");
                partitionList.add("OOFF002");
                partitionList.add("OOFF003");

                //设置表的Hash分区
                Options.addHashPartitions(partitionList, 3);

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
