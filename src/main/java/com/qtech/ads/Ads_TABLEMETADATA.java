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

public class Ads_TABLEMETADATA {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";      //指定kudu地址

//        kuduMaster = "dev02";
        kuduTable = "glm_test";         //指定表名

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

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TABLEID", Type.INT32).key(true).comment("序号").build());

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder(
                        "TABLENAME",Type.STRING).key(true).comment("表名").build());

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder(
                        "SOURCEDB",Type.STRING).nullable(true).comment("来源数据库").build());

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder(
                        "SCHEMANAME",Type.STRING).nullable(true).comment("源表schema名").build());

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder(
                        "TABLETYPE",Type.STRING).nullable(true).comment("表类别").build());

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder(
                        "SOURCEIP",Type.STRING).nullable(true).comment("来源数据库IP地址").build());

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("OGGFULL",Type.STRING).nullable(true).comment("全量文件名").build());

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder(
                        "OGGINCREAMENT",Type.STRING).nullable(true).comment("增量文件名").build());

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder(
                        "LASTUPATETIME",Type.STRING).nullable(true).comment("最后更新时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder(
                        "key",Type.STRING).nullable(true).comment("是否有主键").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder(
                        "into_lake",Type.STRING).nullable(true).comment(
                                "是否入湖").build());


                Schema schema = new Schema(columnSchemas);

                //指定创建表的相关属性
                CreateTableOptions Options = new CreateTableOptions();

                // 设置表的replica备份和分区规则
                ArrayList<String> partitionList = new ArrayList<String>();

                //根据字段id分区
                partitionList.add("TABLEID");
                //设置表的Hash分区
                Options.addHashPartitions(partitionList, 3);

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
