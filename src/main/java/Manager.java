import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 得到hbase admin
 * 创建namespace,table
 */
public class Manager {
    private Connection conn;
    private Admin admin;

    public Manager(Connection conn) throws IOException {
        this.conn = conn;
        admin = conn.getAdmin();
    }

    public Admin getAdmin() throws IOException {
        return admin;
    }

    public void createNS(String nsname) throws IOException {
        NamespaceDescriptor ns = NamespaceDescriptor.create(nsname).build();
        admin.createNamespace(ns);
    }

    /**
     * 创建 只到 columnfamily这一级
     *
     * @param nsname
     * @param tbname
     * @param cfname
     * @throws IOException
     */
    public void createTable(String nsname, String tbname, String cfname) throws IOException {
        HColumnDescriptor cf = new HColumnDescriptor(cfname);
        TableName name = TableName.valueOf(nsname + ":" + tbname);
        HTableDescriptor des = new HTableDescriptor(name);
        des.addFamily(cf);
        admin.createTable(des);
    }

    public void deleteNS(String nsname) throws IOException {
        admin.deleteNamespace(nsname);
    }

    public List<String> listNS(String nsname) throws IOException {
        List<String> nsnames = new ArrayList<>();
        NamespaceDescriptor[] nss = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor ns : nss) {
            nsnames.add(ns.getName());
        }
        return nsnames;
    }
}
