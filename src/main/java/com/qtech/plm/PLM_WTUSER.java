package com.qtech.plm;

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

public class PLM_WTUSER {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";      //指定kudu地址

//        kuduMaster = "dev02";
        kuduTable = "PLM_WTUSER";         //指定表名

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

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("IDA2A2",Type.INT32).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ADMINISTRATIVELOCKISNULL",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TYPEADMINISTRATIVELOCK",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ALLOWLDAPSYNCHRONIZATION",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DISABLED",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLASSNAMEKEYDOMAINREF",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("IDA3DOMAINREF",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("EMAIL",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ENTRYSETADHOCACL",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("EVENTSET",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLASSNAMEKEYFORMAT",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("IDA3FORMAT",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("FULLNAME",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("INHERITEDDOMAIN",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("INTERNAL",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LAST",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LASTSYNCTIME",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NAME",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("REPAIRNEEDED",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SECURITYLABELS",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("STATUS",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CREATESTAMPA2",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MARKFORDELETEA2",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MODIFYSTAMPA2",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLASSNAMEA2A2",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("UPDATECOUNTA2",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("UPDATESTAMPA2",Type.STRING).nullable(true).build());

                Schema schema = new Schema(columnSchemas);

                //指定创建表的相关属性
                CreateTableOptions Options = new CreateTableOptions();

                // 设置表的replica备份和分区规则
                ArrayList<String> partitionList = new ArrayList<String>();

                //根据字段id分区
                partitionList.add("IDA2A2");

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
