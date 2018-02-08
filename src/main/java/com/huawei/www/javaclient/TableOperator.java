import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 直接得到一个table
 */
public class TableOperator {
    private Connection conn;
    private Table table;

    public TableOperator(Connection conn, String tbname, String ns) throws IOException {
        this.conn = conn;
        TableName name = TableName.valueOf(ns + ":" + tbname);
        table = conn.getTable(name);
    }

    public void tablePut(String rowkey, String cf, Map<String, String> keyvalue) throws IOException {
        Put put = new Put(rowkey.getBytes());
        Set<Map.Entry<String, String>> set = keyvalue.entrySet();
        for (Map.Entry<String, String> s : set) {
            put.addColumn(cf.getBytes(), s.getKey().getBytes(), s.getValue().getBytes());
        }
        table.put(put);
    }

    /**
     * 可以添加很多scan条件
     *
     * @param startrow
     * @param endrow
     * @throws IOException
     */
    public List<String> tableScan(String startrow, String endrow, String cf, String qf) throws IOException {
        Scan scan = new Scan(startrow.getBytes(), endrow.getBytes());
        ResultScanner res = table.getScanner(scan);
        Result re = res.next();
        List<String> list = new ArrayList<>();
        while (re.advance()) {
            list.add(new String(re.getValue(cf.getBytes(), qf.getBytes())));
        }
        return list;
    }

    public List<String> tableGet(String row, String cf, String... qf) throws IOException {
        Get get = new Get(row.getBytes());
        for (String s : qf) {
            get.addColumn(cf.getBytes(), s.getBytes());

        }
        Result re = table.get(get);
        List<String> list = new ArrayList<>();
        for (int i = 0; i < re.size(); i++)
            list.add(new String(re.getValue(cf.getBytes(), qf[i].getBytes())));
        return list;
    }

    public void tableDel(String row, String cf, String qf) throws IOException {
        Delete del = new Delete(row.getBytes());
        del.addColumn(cf.getBytes(), qf.getBytes());
        table.delete(del);
    }
}
