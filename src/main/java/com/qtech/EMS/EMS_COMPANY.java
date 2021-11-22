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

public class EMS_COMPANY {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";      //指定kudu地址
//        kuduMaster = "dev02";
        kuduTable = "EMS_COMPANY";         //指定表名

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



    @Test
    public void renameTable() throws KuduException {
        kuduClient.alterTable(kuduTable,new AlterTableOptions().renameTable(
                "ADS_EMS_TEMPERATURE_HUMIDITY"));
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

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("COMPANY_ID", Type.INT32).key(true).comment("公司ID").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("COMPANY_NAME",Type.STRING).nullable(true).comment("公司名称").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ORG_ID",Type.DOUBLE).nullable(true).comment("组织机构ID").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("COMPANY_PROVINCE_ID",Type.DOUBLE).nullable(true).comment("公司所属省份ID").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("COMPANY_PROVINCE_NAME",Type.STRING).nullable(true).comment("公司所属省份名称").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("COMPANY_CITY_ID",Type.DOUBLE).nullable(true).comment("公司所属市ID").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("COMPANY_CITY_NAME",Type.STRING).nullable(true).comment("公司所属市名称").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("COMPANY_DISTRICT_ID",Type.DOUBLE).nullable(true).comment("公司所属ID").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("COMPANY_DISTRICT_NAME",Type.STRING).nullable(true).comment("公司所属区ID").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("COMPANY_LOCATION",Type.STRING).nullable(true).comment("公司所属经纬度").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("COMPANY_ADDRESS",Type.STRING).nullable(true).comment("公司所在地址").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("STATUS",Type.STRING).nullable(true).comment("状态").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CREATE_DT",Type.STRING).nullable(true).comment("创建时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("UPDATE_DT",Type.STRING).nullable(true).comment("更新时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("COMPANY_LOGO",Type.STRING).nullable(true).comment("公司logo").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ORDER",Type.DOUBLE).nullable(true).comment("排序").build());

                Schema schema = new Schema(columnSchemas);

                //指定创建表的相关属性
                CreateTableOptions Options = new CreateTableOptions();

                // 设置表的replica备份和分区规则
                ArrayList<String> partitionList = new ArrayList<String>();

                //根据字段id分区
                partitionList.add("COMPANY_ID");
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
