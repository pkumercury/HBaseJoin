import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

public class HBaseJoin {
    public static class JoinMapper extends
            TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
        String tableName;
        String family1;
        String family2;
        String qualifier1;
        String qualifier2;

        protected void setup(Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.family1 = conf.get("condition.family1");
            this.family2 = conf.get("condition.family2");
            this.qualifier1 = conf.get("condition.qualifier1");
            this.qualifier2 = conf.get("condition.qualifier2");

            TableSplit tableSplit = (TableSplit) context.getInputSplit();
            this.tableName = Bytes.toString(tableSplit.getTableName());

//            System.out.println("Setup");
//            System.out.println(this.family1 + ":" + this.qualifier1 + "\n" + this.family2 + ":" + this.qualifier2);
        }

        @SuppressWarnings("deprecation")
        public void map(ImmutableBytesWritable row, Result values, Context context)
            throws IOException, InterruptedException {
            ImmutableBytesWritable value;
            ImmutableBytesWritable key = null;
            StringBuilder vals = new StringBuilder(this.tableName);

            for (KeyValue kv : values.list()) {
                vals.append(" " + Bytes.toString(kv.getValue()));
                if (this.family1.contains(Bytes.toString(kv.getFamily()))
                        && Bytes.toString(kv.getQualifier()).equals(this.qualifier1)) {
                    key = new ImmutableBytesWritable(kv.getValue());
                }
                else if (this.family2.contains(Bytes.toString(kv.getFamily()))
                        && Bytes.toString(kv.getQualifier()).equals(this.qualifier2)) {
                    key = new ImmutableBytesWritable(kv.getValue());
                }
            }

            value = new ImmutableBytesWritable(vals.toString().getBytes());
            context.write(key, value);

        }
    }

    public static class JoinReducer extends
            TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
        String tableName1;
        String tableName2;
        String[] columns;
        String joinTableName;
        int cnt = 0;

        public void setup(Context context)
            throws IOException, InterruptedException {
            System.out.println("Setup");
            Configuration conf = context.getConfiguration();
            String cols = conf.get("table.columns").trim();
            this.columns = cols.split(" ");

            this.tableName1 = conf.get("table.name1").trim();
            this.tableName2 = conf.get("table.name2").trim();
            this.joinTableName = tableName1 + "_" + tableName2;

        }

        public void reduce(ImmutableBytesWritable Key, Iterable<ImmutableBytesWritable> values, Context context)
            throws IOException, InterruptedException {

            Vector<Vector<String>> vvs1 = new Vector<Vector<String>>();
            Vector<Vector<String>> vvs2 = new Vector<Vector<String>>();

            for (ImmutableBytesWritable value : values) {
                String[] vals = Bytes.toString(value.get()).split(" ");
                Vector<String> vs = new Vector<String>();
                for (int i = 1; i < vals.length; i++) {
                    vs.add(vals[i]);
                }
                if (vals[0].equals(this.tableName1)) {
                    vvs1.add(vs);
                }
                else if (vals[0].equals(this.tableName2)) {
                    vvs2.add(vs);
                }
            }

//            for (Vector<String> vs1 : vvs1) {
//                for (Vector<String> vs2 : vvs2) {
//                    for (String s1 : vs1) {
//                        System.out.print(s1 + " ");
//                    }
//                    for (String s2 : vs2) {
//                        System.out.print(s2 + " ");
//                    }
//                    System.out.println();
//                }
//            }
            for (int i = 0; i < vvs1.size(); i++) {
                for (int j = 0; j < vvs2.size(); j++) {
                    cnt++;
                    for (int k = 0; k < vvs1.get(i).size(); k++) {
                        HBaseUtil.addRecord(joinTableName, String.valueOf(cnt),
                                columns[k], vvs1.get(i).get(k));
                    }
                    for (int k = 0; k < vvs2.get(j).size(); k++) {
                        HBaseUtil.addRecord(joinTableName, String.valueOf(cnt),
                                columns[k + vvs1.get(i).size()], vvs2.get(j).get(k));
                    }
                }
            }
        }
    }

    // 根据tableList生成各步骤的表，并返回修改过的表的列名
    public static ArrayList<ArrayList<String>> preprocess(ArrayList<String> tableList)
        throws IOException {
        ArrayList<ArrayList<String>> aas = new ArrayList<ArrayList<String>>();
        ArrayList<ArrayList<String>> tableFamilies = new ArrayList<ArrayList<String>>();
        for (int i = 0; i < tableList.size(); i++) {
            Set<String> columns = new HBaseMetaData().getColumns(tableList.get(i));
            Set<String> families = new HBaseMetaData().getFamilies(tableList.get(i));
            ArrayList<String> as = new ArrayList<String>(columns);
            ArrayList<String> tf = new ArrayList<String>(families);
            for (int j = 0; j < as.size(); j++) {
                // 重命名列名
                if (!as.get(j).contains(tableList.get(i))) {
                    as.set(j, tableList.get(i) + "_" + as.get(j));
                }
            }
            for (int j = 0; j < tf.size(); j++) {
                if (!tf.get(j).contains(tableList.get(i))) {
                    tf.set(j, tableList.get(i) + "_" + tf.get(j));
                }
            }
            tableFamilies.add(tf);
            aas.add(as);
        }

        StringBuilder sb = new StringBuilder(tableList.get(0));
        ArrayList<String> fams = new ArrayList<String>();
        fams.addAll(tableFamilies.get(0));
        for (int i = 1; i < tableList.size(); i++) {
            sb.append("_" + tableList.get(i));
            fams.addAll(tableFamilies.get(i));
            HBaseUtil.createTable(sb.toString(), fams);
        }
        return aas;
    }

    public static void run(ArrayList<String> tableList, ArrayList<String> conditionList)
        throws IOException{
        ArrayList<ArrayList<String>> tableColumns = preprocess(tableList);
        ArrayList<String> currentCols = new ArrayList<String>();
        StringBuilder currentTableName = new StringBuilder(tableList.get(0));
        currentCols.addAll(tableColumns.get(0));

        for (int i = 1; i < tableColumns.size(); i++) {
            currentCols.addAll(tableColumns.get(i));

            // 解析连接条件的各个部分
            String condition = conditionList.get(i - 1);
            String family1, family2, qualifier1, qualifier2;
            String[] t = condition.split("=");
            String table1 = t[0].split("\\.")[0];
            qualifier1 = t[0].split("\\.")[1];
            String table2 = t[1].split("\\.")[0];
            qualifier2 = t[1].split("\\.")[1];
            family1 = table1 + "_" + qualifier1.split(":")[0];
            qualifier1 = qualifier1.split(":")[1];
            family2 = table2 + "_" + qualifier2.split(":")[0];
            qualifier2 = qualifier2.split(":")[1];

            join(currentTableName.toString(), tableList.get(i), family1, family2,
                    qualifier1, qualifier2, currentCols);

            currentTableName.append("_" + tableList.get(i));
        }
    }


    public static void join(String table1, String table2, String family1, String family2,
                           String qualifier1, String qualifier2, ArrayList<String> cols) {
        //创建配置信息
        Configuration conf = HBaseConfiguration.create();
        //生成数据源
        List<Scan> scans = new ArrayList<Scan>();
        Scan scan1 = new Scan();
        scan1.setCaching(100);
        scan1.setCacheBlocks(false);
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, table1.getBytes());
        scans.add(scan1);

        Scan scan2 = new Scan();
        scan2.setCaching(100);
        scan2.setCacheBlocks(false);
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,  table2.getBytes());
        scans.add(scan2);
        try {

            StringBuilder colsString = new StringBuilder("");
            for (String col : cols) {
                colsString.append(col + " ");
            }
            conf.set("table.columns", colsString.toString());
            conf.set("condition.family1", family1);
            conf.set("condition.family2", family2);
            conf.set("condition.qualifier1", qualifier1);
            conf.set("condition.qualifier2", qualifier2);
            conf.set("table.name1", table1);
            conf.set("table.name2", table2);

            @SuppressWarnings("deprecation")
            // 创建作业对象
            Job job = new Job(conf);
            job.setInputFormatClass(MultiTableInputFormat.class);
            TableMapReduceUtil.initTableMapperJob(scans, HBaseJoin.JoinMapper.class,
                    ImmutableBytesWritable.class,ImmutableBytesWritable.class, job);
            TableMapReduceUtil.initTableReducerJob("Result", HBaseJoin.JoinReducer.class, job);
            TableMapReduceUtil.addDependencyJars(job.getConfiguration(), ImmutableBytesWritable.class);

            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ArrayList<String> tableList = new ArrayList<String>();
        tableList.add("Person");
        tableList.add("Gender");
        tableList.add("Hobby");
        ArrayList<String> conditionList = new ArrayList<String>();
        conditionList.add("Person.info:id=Gender.info:id");
        conditionList.add("Person.info:name=Hobby.info:name");
        try {
            run(tableList, conditionList);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}