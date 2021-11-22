package com.qtech.plm;

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

public class createTableCameraamform {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                //初始化操作
        kuduMaster = "bigdata01,bigdata02,bigdata03";      //指定kudu地址
//        kuduMaster = "dev02";
        kuduTable = "PLM_CAMERAAMFORM";         //指定表名

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

    @Test
    public void renameTable() throws KuduException {
        kuduClient.alterTable(kuduTable, new AlterTableOptions().renameTable(
                "PLM_CAMERAAMFORM"));
    }

    @Test
    /**
     * 字段
     */
    public void alterColumn() {

        //创建非空字段
//        try {
//            int l = 0;
//            kuduClient.alterTable(kuduTable,new AlterTableOptions().addColumn(
//                    "des_id",Type.INT32,l));
//        } catch (KuduException e) {
//            e.printStackTrace();
//        }

        //创建默认为空字段
        try {
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("ISSUECODE", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("ISSUECODE1", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("ISSUECODE2", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("CSREASON1", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("CSREASON2", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("CSREASON3", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("CSREASON4", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("CSREASON5", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("CSREASON6", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("CSREASON7", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("CSREASON8", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("CSREASON9", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("LCREASON1", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("LCREASON2", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("LCREASON3", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("LCREASON4", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("LCREASON5", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("LCREASON6", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("LCREASON7", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("LCREASON8", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("LCREASON9", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("CSREASONSOLUTION1", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("CSREASONSOLUTION2", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("CSREASONSOLUTION3", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("LCREASONSOLUTION1", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("LCREASONSOLUTION2", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("LCREASONSOLUTION3", Type.STRING));
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    /**
     * kudu创建表
     */
    @Test
    public void create_Table() {

        try {

            //判断表是否存在
            if (!kuduClient.tableExists(kuduTable)) {

                //设置表的schema
                ArrayList<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("IDA2A2", Type.INT32).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("AMDESCRIPTION", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("BADPHENOMENON", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CASETHAT", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLOSLEVEL", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CODE1", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CODE2", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CODE3", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CONTENT", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DOCID", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DUTY1", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DUTY2", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DUTY3", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DUTYAUDIT", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DUTYAUDITNAME", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DUTYAUDITSCORE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DUTYONER", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DUTYONERNAME", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DUTYONERSCORE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DUTYREASON", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DUTYSCOPE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DUTYUSER", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DUTYUSER2", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DUTYUSER3", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("EQUIPMENTISSUE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("FIRSTDEAL", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("FIRSTREASON", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("FORMCREATETIME", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("FORMCREATOR", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("FORMNUMBER", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("FORMTEMPLATEVERSION", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("HANDLING", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ISAMTIME", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ISFOURH", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ISMERGEWORKNO", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ISOFFLINE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ISQUALITYBREACH", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ISSUBSIDE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LASTMODIFIER", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LASTMODIFYTIME", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LCREASON", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LOCATION", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MACHINENO", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MACHINETYPE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MASTERID", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MATISSUEMODEL", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MATISSUENAME", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MATISSUETYPE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MATISSUEVENDOR", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MATEPROCESSMODE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MATERIALISSUE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MATERIALNAME", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MATERIALTYPE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MATERIALVENDOR", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MERGEWORKNO", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NG", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NGRATE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("NGREASON", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PERPETUALRESOURCE1", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PERPETUALRESOURCE2", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PERPETUALRESOURCE3", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PHENOMENON", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLASSNAMEKEYA4", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("IDA3A4", Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SHIFTCODE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SMSMATHOUR_1", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SMSMATHOUR_2", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SMSMATHOUR_3", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SMSMATHOUR_4", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SMSMATHOUR_5", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SMSMATHOUR_6", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SMSMATQUAL_1", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SMSMATQUAL_2", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SMSMATQUAL_3", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SMSMATQUAL_4", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CREATESTAMPA2", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MARKFORDELETEA2", Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MODIFYSTAMPA2", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLASSNAMEA2A2", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("UPDATECOUNTA2", Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("UPDATESTAMPA2", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TOTAL", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("BRANCHIDA2TYPEDEFINITIONREFE", Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("IDA2TYPEDEFINITIONREFERENCE", Type.DOUBLE).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("VERSIONNUMBER", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WORKDESCRIPTION", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WORKLEVEL", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WORKMO", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WORKMODEL", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WORKNO", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WORKRCARDNO", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WORKSTAGE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WORKSHOPCODE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLAIM_MATTYPE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLAIM_VENDOR", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLAIM_REASON", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLAIM_QTMODEL", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLAIM_WORKTIME", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLAIM_UNITPRICE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLAIM_MONEY", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLAIM_OFFLINE_REASON", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLAIM_OFFLINE_QTMODEL", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLAIM_OFFLINE_WORKTIME", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLAIM_OFFLINE_UNITPRICE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLAIM_OFFLINE_MONEY", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLAIM_VENDOR01", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CLAIM_MATTYPE01", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ISCLAIM", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("QAUSERID", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("PROCESSUSERID", Type.STRING).nullable(true).build());

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
