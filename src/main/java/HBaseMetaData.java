/**
 * Created by wen on 17-1-16.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class HBaseMetaData {
    private HBaseAdmin hBaseAdmin;
    private Configuration hBaseConfiguration;

    @SuppressWarnings("deprecation")
    public HBaseMetaData () throws IOException {
        this.hBaseConfiguration = HBaseConfiguration.create();
        this.hBaseAdmin = new HBaseAdmin(hBaseConfiguration);
    }
    /** get all Table names **/
    public List<String> getTableNames(String regex) throws IOException {
        Pattern pattern=Pattern.compile(regex);
        List<String> tableList = new ArrayList<String>();
        TableName[] tableNames=hBaseAdmin.listTableNames();
        for (TableName tableName:tableNames){
            if(pattern.matcher(tableName.toString()).find()){
                tableList.add(tableName.toString());
            }
        }
        return tableList;
    }
    /** Get all columns **/
    public Set<String> getColumns(String hbaseTable) throws IOException {
        return getColumns(hbaseTable, 10000);
    }
    /** get all columns from the table **/
    @SuppressWarnings("deprecation")
    public Set<String> getColumns(String hbaseTable, int limitScan) throws IOException {
        Set<String> columnList = new TreeSet<String>();
        HTable hTable=new HTable(hBaseConfiguration, hbaseTable);
        Scan scan=new Scan();
        scan.setFilter(new PageFilter(limitScan));
        ResultScanner results = hTable.getScanner(scan);
        for(Result result:results){
            for(KeyValue keyValue:result.list()){
                columnList.add(
                        new String(keyValue.getFamily()) + ":" +
                                new String(keyValue.getQualifier())
                );
            }
        }
        return columnList;
    }

    @SuppressWarnings("deprecation")
    public Set<String> getFamilies(String hbaseTable)
        throws IOException {
        Set<String> families = new TreeSet<String>();
        HTable hTable = new HTable(hBaseConfiguration, hbaseTable);
        Scan scan = new Scan();
        ResultScanner results = hTable.getScanner(scan);
        for(Result result:results){
            for(KeyValue keyValue:result.list()){
                families.add(new String(keyValue.getFamily()));
            }
        }
        return families;
    }

}

