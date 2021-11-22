import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class kuDuTest {
    private KuduClient kuduClient;
    private String kuduMaster;
    private String kuduTable;

    @Before
    public void init() {                //初始化操作
//        kuduMaster = "10.170.3.134:7051";      //指定kudu地址
        kuduMaster = "bigdata01,bigdata02,bigdata03";
        kuduTable = "SRM_VENDOR";         //指定表名

        //创建连接
        KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMaster);

        kuduClientBuilder.defaultAdminOperationTimeoutMs(8000);

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
     * kudu删除表
     * @throws KuduException
     */
    @Test
    public void deleteTable() throws KuduException {
        if (kuduClient.tableExists(kuduTable)){
            kuduClient.deleteTable(kuduTable);
        }
    }

    @Test
    public void renameTable(){
        try {
            kuduClient.alterTable(kuduTable,
                    new AlterTableOptions().renameTable("ADS_SPCMESRESULTLIST"));
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

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ID", Type.INT64).key(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("DDATE", Type.STRING ).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ITEM", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CUST_CODE", Type.STRING).nullable(true).build());
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("RN", Type.STRING).nullable(true).build());
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

    @Test
    /**
     * 字段
     */
    public void alterColumn(){

        //创建非空字段
//        try {
//            int l = 0;
//            kuduClient.alterTable(kuduTable,new AlterTableOptions().addColumn(
//                    "des_id",Type.INT32,l));
//        } catch (KuduException e) {
//            e.printStackTrace();
//        }

        //创建默认为空字段

//        Date dNow = new Date( );
//        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
        try {
//            kuduClient.alterTable(kuduTable, new AlterTableOptions().dropColumn("PERIOD"));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("ISZSCQGLZD", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("ISZSCQDEPT", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("ZSCQJFDESC", Type.STRING));
            kuduClient.alterTable(kuduTable, new AlterTableOptions().addNullableColumn("ORIGINFACTORYNAME", Type.STRING));
//            kuduClient.alterTable(kuduTable, new AlterTableOptions().dropColumn("MAJOR_CATEGORIES"));

//            kuduClient.alterTable(kuduTable, new ReplicaSelection(kuduTable));
        } catch (KuduException e) {
            e.printStackTrace();
        }





        //删除字段，但是主键不能删除

//        try {
//            kuduClient.alterTable(kuduTable,new AlterTableOptions().dropColumn("part_spec"));
//        } catch (KuduException e) {
//            e.printStackTrace();
//        }

    }


    /**
     * 添加range分区
     */
    @Test
    public void alterRange() {
        ArrayList<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();

        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("ID",Type.INT32).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TESTITEM",Type.STRING).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TESTSUBITEM",Type.STRING).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TESTCONDITION",Type.STRING).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("LOWER",Type.STRING).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("UPPER",Type.STRING).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("UNIT",Type.STRING).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SUBITEMTESTVALUE",Type.STRING).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("SUBITEMTESTRESULT",Type.STRING).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("TESTTIME",Type.STRING).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MEMO1",Type.STRING).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MEMO2",Type.STRING).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MEMO3",Type.STRING).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MEMO4",Type.STRING).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("MEMO5",Type.STRING).key(true).build());

        Schema schema = new Schema(columnSchemas);
        //指定创建表的相关属性
        CreateTableOptions Options = new CreateTableOptions();

        // 设置表的replica备份和分区规则
        ArrayList<String> partitionList = new ArrayList<String>();

        //根据字段id分区
        partitionList.add("ID");

        //设置表的Hash分区
        Options.addHashPartitions(partitionList, 5);

        //设置表的备份数
        Options.setNumReplicas(3);

        //设置range分区
//                Options.setRangePartitionColumns(partitionList);

        String[] keys = new String[]{"2021-03-01","2021-03-15","2021-04-01","2021-04-15","2021-05-01","2021-05-15","2021-06-01","2021-06-15","2021-06-30"};
        for (String key : keys) {
            PartialRow partialRow = schema.newPartialRow();
            partialRow.addString("TESTTIME", key);
            AlterTableOptions alterTableOptions = new AlterTableOptions();
//            alterTableOptions.addRangePartition()
            Options.addSplitRow(partialRow);
        }
        ArrayList<String> partitionList1 = new ArrayList<String>();

        partitionList1.add("TESTTIME");
        Options.setRangePartitionColumns(partitionList1);



//        kuduClient.alterTable(kuduTable,);
    }

    /**
     * 向kudu插入数据
     * @throws KuduException
     */
    @Test
    public void insertData() throws KuduException {
        //向表加载数据需要一个Session对象
        KuduSession kuduSession = kuduClient.newSession();
        //设置kudu提交数据方式为自动flush
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

        //打开本次操作的表名
        KuduTable kuduTable = kuduClient.openTable(this.kuduTable);

        for (int i = 1; i < 10; i++) {
            //需要使用kuduTable 来构建operation 的子类实例对象，此处是insert操作
            Insert insert = kuduTable.newInsert();
            PartialRow row = insert.getRow();
            row.addInt("id",i);
            row.addString("name","zahngsan-"+i);
            row.addInt("age",20+i);
            row.addInt("sex",20-i);
            kuduSession.apply(insert);

        }
    }

    /**
     * 查询数据
     */
    @Test
    public void queryData() throws KuduException {
        //构建一个查询的扫描器
        KuduScanner.KuduScannerBuilder kuduScannerBuilder = kuduClient.newScannerBuilder(kuduClient.openTable(kuduTable));
        ArrayList<String> columnList = new ArrayList<String>();
        columnList.add("ID");
        columnList.add("STARTTIME");
        kuduScannerBuilder.setProjectedColumnNames(columnList);
        //返回结果集
        KuduScanner build = kuduScannerBuilder.build();

        while(build.hasMoreRows()){
            RowResultIterator rowResults = build.nextRows();
            while (rowResults.hasNext()){
                RowResult row = rowResults.next();
                String id = row.getString("ID");
                System.out.println("id="+id);
            }
        }
    }

    /**
     * kudu更新数据
     * @throws KuduException
     */
    @Test
    public void updataData() throws KuduException {

        //构建kuduseeion对象
        KuduSession kuduSession = kuduClient.newSession();

        //设置刷写方式
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        //需要使用 kuduTable 来构建 Operation 的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(this.kuduTable);
        Update update = kuduTable.newUpdate();
        PartialRow row = update.getRow();
        row.addInt("id",1);
        row.addString("name","zhangsan-1");
        row.addInt("age",100);
        row.addInt("sex",1);

        kuduSession.apply(update);
    }

    /**
     * kudu更新数据
     * @throws KuduException
     */
    @Test
    public void upsertData() throws KuduException {
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        //需要使用 kuduTable 来构建 Operation 的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(this.kuduTable);
        Upsert upsert = kuduTable.newUpsert();
        PartialRow row = upsert.getRow();
        row.addInt("id",100);
        row.addString("name","zhangsan-1");
        row.addInt("age",100);
        row.addInt("sex",1);

        kuduSession.apply(upsert);
    }

    /**
     * kudu删除数据
     * @throws KuduException
     */
    @Test
    public void deleteData() throws KuduException {
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        //需要使用 kuduTable 来构建 Operation 的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(this.kuduTable);
        Delete delete = kuduTable.newDelete();
        PartialRow row = delete.getRow();
        row.addInt("id",100);

        kuduSession.apply(delete);
    }



}