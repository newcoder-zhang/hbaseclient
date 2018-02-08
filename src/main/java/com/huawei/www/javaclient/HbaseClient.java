import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class HbaseClient {
    public static void main(String[] args) throws IOException {
        Connection conn = ConnectionFactory.createConnection();
        Manager manager=new Manager(conn);
         //manager.createNS("ns3");
       //manager.createTable("ns3","t1","cf");

        TableOperator opt=new TableOperator(conn,"t1","ns3");
        HashMap<String,String> map=new HashMap<>();
        map.put("name","zl");
        map.put("age","18");
        map.put("sex","male");
        opt.tablePut("2018","cf",map);
        List<String> list = opt.tableGet("2018", "cf", "name","age","sex");
        for (String s:list) {
            System.out.println(s);
        }
    }
}
