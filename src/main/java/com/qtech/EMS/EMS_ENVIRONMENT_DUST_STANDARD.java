package com.qtech.EMS;

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

public class EMS_ENVIRONMENT_DUST_STANDARD {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";      //指定kudu地址
//        kuduMaster = "dev02";
        kuduTable = "EMS_ENVIRONMENT_DUST_STANDARD";         //指定表名

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
    public void creatTable(){

        try {

            //判断表是否存在
            if (!kuduClient.tableExists(kuduTable)){

                //设置表的schema
                ArrayList<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ID",Type.INT32).key(true).comment("自增主键").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TITLE",
                        Type.STRING).nullable(true).comment("级别名称：10级，100级，1000" +
                        "级").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LEVEL3_WARN_MAX",Type.DOUBLE).nullable(true).comment("0.3μm超标上限").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LEVEL3_ALARM_MAX",Type.DOUBLE).nullable(true).comment("0.3μm警示上限").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LEVEL5_WARN_MAX",Type.DOUBLE).nullable(true).comment("0.5μm超标上限").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LEVEL5_ALARM_MAX",Type.DOUBLE).nullable(true).comment("0.5μm警示上限").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CREATE_BY",Type.STRING).nullable(true).comment("添加人工号").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("UPDATE_BY",Type.STRING).nullable(true).comment("修改人工号").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CREATE_TIME",Type.STRING).nullable(true).comment("记录创建时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("UPDATE_TIME",Type.STRING).nullable(true).comment("").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("IS_DELETED",Type.DOUBLE).nullable(true).comment("是否已删除。0：否、1：是").build());

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
