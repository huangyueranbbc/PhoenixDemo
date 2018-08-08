package com.hyr.phoenix.demo.udf;

import com.hyr.phoenix.demo.Conf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

/*******************************************************************************
 * @date 2018-08-07 上午 10:59
 * @author: huangyueran
 * @Description: phoenix SQL Function 自定义函数 UDF
 ******************************************************************************/
public class FunctionOperation {
    private final Logger log = LoggerFactory.getLogger(Conf.class);

    private Connection connection = null;
    private Statement statement = null;
    private Long startTime;

    @Before
    public void init() {
        try {
            Class.forName(Conf.driver);
            Properties props = new Properties();
            props.setProperty("phoenix.functions.allowUserDefinedFunctions", "true");
            connection = DriverManager.getConnection(Conf.url, props);
            startTime = System.currentTimeMillis();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * hadoop dfs -put MyCustomFunction.jar /hbase/lib
     * <p>
     * hbase-site.xml add->
     * <property>
     * <name>phoenix.functions.allowUserDefinedFunctions</name>
     * <value>true</value>
     * </property>
     *
     * <property>
     * <name>fs.hdfs.impl</name>
     * <value>org.apache.hadoop.hdfs.DistributedFileSystem</value>
     * </property>
     *
     * <property>
     * <name>hbase.local.dir</name>
     * <value>hdfs://master:9000/hbase/local/</value>
     * <description>Directory on the local filesystem to be used
     * as a local storage.</description>
     * </property>
     *
     * <property>
     * <name>hbase.dynamic.jars.dir</name>
     * <value>hdfs://master:9000/hbase/lib</value>
     * <description>
     * The directory from which the custom udf jars can be loaded
     * dynamically by the phoenix client/region server without the need to restart. However,
     * an already loaded udf class would not be un-loaded. See
     * HBASE-1936 for more details.
     * </description>
     * </property>
     */
    @Test
    public void createFunction() {
        try {
            statement = connection.createStatement();
            String sql = "CREATE FUNCTION charformat(varchar) returns varchar as 'com.hyr.phoenix.demo.udf.MyCustomFunction' using jar 'hdfs://master:9000/hbase/lib/MyCustomFunction.jar'";
            statement.executeUpdate(sql);

            long costTime = System.currentTimeMillis() - startTime;
            log.info("create function costTime:{}", costTime);
        } catch (SQLException e) {
            log.error("create function error!", e);
        }
    }

    @Test
    public void useFunction() {
        try {
            statement = connection.createStatement();
            String sql = "select charformat(DOMAIN) from WEB_STAT";
            ResultSet rs = statement.executeQuery(sql);

            while (rs.next()) {
                System.out.println(rs.getString(1));
            }

            long costTime = System.currentTimeMillis() - startTime;
            log.info("use function costTime:{}", costTime);
        } catch (SQLException e) {
            log.error("use function error!", e);
        }
    }

    @Test
    public void dropFunction() {
        try {
            statement = connection.createStatement();
            String sql = "DROP FUNCTION charformat";
            statement.execute(sql);

            long costTime = System.currentTimeMillis() - startTime;
            log.info("drop function costTime:{}", costTime);
        } catch (SQLException e) {
            log.error("drop function error!", e);
        }
    }

    @After
    public void close() {
        if (statement != null) {
            try {
                statement.close();
                statement = null;
            } catch (SQLException e) {
                log.error("statement close error!", e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
                connection = null;
            } catch (SQLException e) {
                log.error("connection close error!", e);
            }
        }
    }

}
