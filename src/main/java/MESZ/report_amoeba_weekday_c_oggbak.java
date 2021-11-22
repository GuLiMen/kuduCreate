package MESZ;

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

public class report_amoeba_weekday_c_oggbak {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                                    //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";       //指定kudu地址
//        kuduMaster = "dev02";
        kuduTable = "CS_WORK_LOG";                          //指定表名

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
    public void renameTable(){
        try {
            kuduClient.alterTable(kuduTable,
                    new AlterTableOptions().renameTable("BI_QUALITY_KEY_MATERIAL_TYPE"));
        } catch (KuduException e) {
            e.printStackTrace();
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

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("logID",
                        Type.INT32).key(true).comment("流水ID").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("work_id",Type.INT32).nullable(true).comment("对应work_info ID").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("status",Type.STRING).nullable(true).comment("工作状态").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("startDate",Type.STRING).nullable(true).comment("开始时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("endDate",Type.STRING).nullable(true).comment("结束时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("realDate",Type.STRING).nullable(true).comment("实际结束时间").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("role",Type.STRING).nullable(true).comment("角色").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("executeUser",Type.STRING).nullable(true).comment("执行人（ID）").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("executeUserName",Type.STRING).nullable(true).comment("执行人（名字）").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("comment",Type.STRING).nullable(true).comment("审核意见").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("active",Type.STRING).nullable(true).comment("是否可审核").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("preLogID",Type.STRING).nullable(true).comment("前置ID").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("attr1",Type.STRING).nullable(true).comment("签核回复字段").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("attr2",Type.STRING).nullable(true).comment("签核回复字段").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("attr3",Type.STRING).nullable(true).comment("签核回复字段").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("attr4",Type.STRING).nullable(true).comment("签核回复字段").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("attr5",Type.STRING).nullable(true).comment("签核回复字段").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SSS",Type.STRING).nullable(true).comment("签核回复字段").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("attr6",Type.STRING).nullable(true).comment("签核回复字段").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("attr7",Type.STRING).nullable(true).comment("签核回复字段").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("attr8",Type.STRING).nullable(true).comment("签核回复字段").build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("responsibleDepartment",Type.STRING).nullable(true).comment("责任单位").build());

                Schema schema = new Schema(columnSchemas);

                //指定创建表的相关属性
                CreateTableOptions Options = new CreateTableOptions();

                // 设置表的replica备份和分区规则
                ArrayList<String> partitionList = new ArrayList<String>();

                //根据字段id分区
                partitionList.add("logID");
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
