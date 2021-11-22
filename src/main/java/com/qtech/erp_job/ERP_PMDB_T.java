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

public class ERP_PMDB_T {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                                    //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";       //指定kudu地址
//        kuduMaster = "dev02";
        kuduTable = "ERP_PMDB_T";                          //指定表名

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

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBENT",Type.INT64).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBDOCNO",Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBSEQ",Type.INT64).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBSITE",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB001",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB002",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB003",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB004",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB005",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB006",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB007",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB008",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB009",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB010",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB011",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB012",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB014",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB015",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB016",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB017",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB018",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB019",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB020",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB021",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB030",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB031",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB032",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB033",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB034",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB035",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB036",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB037",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB038",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB039",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB040",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB041",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB042",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB043",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB044",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB045",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB046",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB048",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB049",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB050",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB051",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB200",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB201",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB202",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB203",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB204",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB205",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB206",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB207",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB208",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB209",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB210",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB211",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB212",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB250",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB251",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB252",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB253",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB254",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB255",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB256",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB257",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB258",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB259",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB900",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB999",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD001",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD002",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD003",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD004",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD005",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD006",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD007",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD008",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD009",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD010",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD011",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD012",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD013",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD014",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD015",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD016",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD017",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD018",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD019",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD020",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD021",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD022",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD023",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD024",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD025",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD026",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD027",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD028",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD029",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUD030",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB260",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB052",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB227",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB053",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB213",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB054",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB214",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB261",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUA001",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUA002",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDBUA003",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB055",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB056",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB057",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB058",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB059",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB060",Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PMDB061",Type.DOUBLE).nullable(true).build());

                Schema schema = new Schema(columnSchemas);

                //指定创建表的相关属性
                CreateTableOptions Options = new CreateTableOptions();

                // 设置表的replica备份和分区规则
                ArrayList<String> partitionList = new ArrayList<String>();

                //根据字段id分区
                partitionList.add("PMDBENT");
                partitionList.add("PMDBDOCNO");
                partitionList.add("PMDBSEQ");
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
