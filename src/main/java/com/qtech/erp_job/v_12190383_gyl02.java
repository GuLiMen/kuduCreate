package com.qtech.erp_job;

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

public class v_12190383_gyl02 {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                                    //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";       //指定kudu地址
//        kuduMaster = "dev02";
        kuduTable = "erp_v_12190383_gyl02";                          //指定表名

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

/*    @Test
    public void renameTable(){
        try {
            kuduClient.alterTable(kuduTable,
                    new AlterTableOptions().renameTable("BI_QUALITY_KEY_MATERIAL_TYPE"));
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }*/

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


                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("bmba001", Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("bmba003",Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("bmba001b",Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ddate",Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("qt_spec",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("qt_spec02",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("qty",Type.DOUBLE).nullable(true).build());

                Schema schema = new Schema(columnSchemas);

                //指定创建表的相关属性
                CreateTableOptions Options = new CreateTableOptions();

                // 设置表的replica备份和分区规则
                ArrayList<String> partitionList = new ArrayList<String>();

                //根据字段id分区
                partitionList.add("bmba001");
                partitionList.add("bmba003");
                partitionList.add("bmba001b");
                partitionList.add("ddate");
                //设置表的Hash分区
                Options.addHashPartitions(partitionList,8);

                //设置表的备份数
                Options.setNumReplicas(3);

                //创建表
                kuduClient.createTable(kuduTable, schema, Options);
            }
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }
}
