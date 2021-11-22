package com.qtech.EMS;

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

public class ENVIRONMENT_BASEINFO {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";      //指定kudu地址
//        kuduMaster = "dev02";
        kuduTable = "EMS_ENVIRONMENT_BASEINFO_GLM";         //指定表名

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

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ID",
                        Type.INT32).key(true).comment("自增主键").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DEVICE_ID",Type.DOUBLE).nullable(true).comment("设备ID,关联台账表中的DEVICE_ID").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DEVICE_TYPE",Type.DOUBLE).nullable(true).comment("设备类型，1：温湿度，2：尘埃PARTICLE").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DUST_STANDARD",Type.DOUBLE).nullable(true).comment("添加尘埃设备时必须使用的字段，关联ENVIRONMENT_DUST_STANDARD表的主键ID").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TITLE",Type.STRING).nullable(true).comment("监测点名称").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DESCRIPTION",Type.STRING).nullable(true).comment("描述").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WARN_MAX",Type.DOUBLE).nullable(true).comment("温度超标上限").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WARN_MIN",Type.DOUBLE).nullable(true).comment("温度超标下限").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ALARM_MAX",Type.DOUBLE).nullable(true).comment("温度警示上限").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ALARM_MIN",Type.DOUBLE).nullable(true).comment("温度警示下限").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("HUMIDITY_WARN_MAX",Type.DOUBLE).nullable(true).comment("湿度超标上限").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("HUMIDITY_WARN_MIN",Type.DOUBLE).nullable(true).comment("湿度超标下限").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("HUMIDITY_ALARM_MAX",Type.DOUBLE).nullable(true).comment("湿度警示上限").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("HUMIDITY_ALARM_MIN",Type.DOUBLE).nullable(true).comment("湿度警示下限").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("HIERARCHY_CODE",Type.STRING).nullable(true).comment("层级CODE，区域CODE").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CREATE_BY",Type.STRING).nullable(true).comment("添加人工号").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("UPDATE_BY",Type.STRING).nullable(true).comment("修改人工号").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CREATE_TIME",Type.STRING).nullable(true).comment("").build());
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
