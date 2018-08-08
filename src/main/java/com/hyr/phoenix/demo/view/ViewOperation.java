package com.hyr.phoenix.demo.view;

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
public class ViewOperation {
    private final Logger log = LoggerFactory.getLogger(Conf.class);

    private Connection connection = null;
    private Statement statement = null;
    private Long startTime;

    @Before
    public void init() {
        try {
            Class.forName(Conf.driver);
            connection = DriverManager.getConnection(Conf.url);
            startTime = System.currentTimeMillis();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createView() {
        try {
            statement = connection.createStatement();
            String sql = "CREATE VIEW my_web_stat_view AS SELECT * from WEB_STAT WHERE DB > 500";
            statement.executeUpdate(sql);

            long costTime = System.currentTimeMillis() - startTime;
            log.info("create view costTime:{}", costTime);
        } catch (SQLException e) {
            log.error("create view error!", e);
        }
    }

    @Test
    public void useView() {
        try {
            statement = connection.createStatement();
            String sql = "select * from my_web_stat_view";
            ResultSet rs = statement.executeQuery(sql);

            while (rs.next()) {
                String host = rs.getString("HOST");
                System.out.println("host = " + host);
                String domain = rs.getString("DOMAIN");
                System.out.println("domain = " + domain);
                String feature = rs.getString("FEATURE");
                System.out.println("feature = " + feature);
                Date date = rs.getDate("DATE");
                System.out.println("date = " + date);
                long core = rs.getLong("CORE");
                System.out.println("core = " + core);
                long db = rs.getLong("DB");
                System.out.println("db = " + db);
                int active_visitor = rs.getInt("ACTIVE_VISITOR");
                System.out.println("active_visitor = " + active_visitor);
                System.out.println("===============================");
            }

            long costTime = System.currentTimeMillis() - startTime;
            log.info("use view costTime:{}", costTime);
        } catch (SQLException e) {
            log.error("use view error!", e);
        }
    }

    @Test
    public void dropView() {
        try {
            statement = connection.createStatement();
            String sql = "DROP VIEW my_web_stat_view";
            statement.execute(sql);

            long costTime = System.currentTimeMillis() - startTime;
            log.info("drop view costTime:{}", costTime);
        } catch (SQLException e) {
            log.error("drop view error!", e);
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
