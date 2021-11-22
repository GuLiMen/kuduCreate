package com.qtech.AA;

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

public class AA_REJECT_GC_TEST {

    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                                    //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";       //指定kudu地址
//        kuduMaster = "dev02";
        kuduTable = "AA_REJECT_GC_TEST";                          //指定表名

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
                    new AlterTableOptions().renameTable("erp_v_12190383_bom_one022"));
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
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("sid",Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("main_aa_start",Type.STRING).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("cob",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("device_num",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("eid",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("station",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("area",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("date_dir",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("file",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("parse_time",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("process_type",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("working_shift",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("pathdate",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("specialprocess",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("init_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("reinit_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("aa3_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("mtf_check2_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("chart_alignment2_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("epoxy_inspection_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("sensorid_check_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("vcm_init_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("vcm_init2_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("vcm_init3_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("vcm_poweron_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("vcm_poweron2_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("vcm_poweron3_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("vcm_hall_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("vcm_hall2_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("vcm_move_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("vcm_move2_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("init_check_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("blemish_defect_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("lightpanel_oc_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("saveoc_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("sensor_throw_trigger",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("uv_mtf_check2_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("gripperopen_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("vcm_ois_init_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("vcm_ois_init2_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("sid_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("main_aa_start_exist",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("subpath",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("lot",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("unit",Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("y_level_fail_cnt",Type.INT8).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ATTRIBUTE",Type.STRING).nullable(true).build());

                Schema schema = new Schema(columnSchemas);

                //指定创建表的相关属性
                CreateTableOptions Options = new CreateTableOptions();

                // 设置表的replica备份和分区规则
                ArrayList<String> partitionList = new ArrayList<String>();

                //根据字段id分区
                partitionList.add("sid");
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
