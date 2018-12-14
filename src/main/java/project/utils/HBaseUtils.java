package project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author YaboSun
 *
 * HBase操作工具类
 * Java工具类建议采用单例模式
 */
public class HBaseUtils {
    // 基本配置
    HBaseAdmin admin = null;
    Configuration configuration = null;

    /**
     * 私有构造方法
     */
    private HBaseUtils() {
        // 初始化configuration
        configuration = new Configuration();
        // 这里的配置文件主要是将$HBASE_HOME/conf/hbase-site.xml中的配置进行设置
        configuration.set("hbase.zookeeper.quorm", "leader:2181");
        configuration.set("hbase.rootdir", "hdfs://leader:2181/hbase");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 单例模式设计
    private static HBaseUtils instance = null;
    // 加synchronized防止有线程安全问题
    public static synchronized HBaseUtils getInstance() {
        if (instance == null) {
            instance = new HBaseUtils();
        }
        return instance;
    }

    /**
     * 根据表名获取HTable实例
     * @param tableName 表名
     * @return HTable实例
     */
    public HTable getTable(String tableName) throws IOException {
        HTable table = null;
        table = new HTable(configuration, tableName);
        return table;
    }

    /**
     * 添加一条记录到HBase表
     * @param tableName 表名
     * @param rowkey HBase表的rowkey
     * @param cf 列族名
     * @param column 列名
     * @param value 要插入的值
     */
    public void put(String tableName, String rowkey, String cf, String column, String value) throws IOException {
        // 获得表名
        HTable table = getTable(tableName);
        // 将参数放入
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));

        table.put(put);

    }
    public static void main(String[] args) throws IOException {
        // 测试获取表名
        //HTable hTable = HBaseUtils.getInstance().getTable("course_clickcount");
        //System.out.println(hTable.getName().getNameAsString());

        // 测试将数据放入HBase
        String tableName = "course_clickcount";
        String rowkey = "20181111_88";
        String cf = "log_info";
        String column = "click_count";
        String value = "2";
        HBaseUtils.getInstance().put(tableName, rowkey, cf, column, value);
    }
}
