package com.hyr.phoenix.demo.table;

import com.hyr.phoenix.demo.Conf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/*******************************************************************************
 * @date 2018-08-07 上午 10:59
 * @author: huangyueran
 * @Description: PhoenixSQL查询
 ******************************************************************************/
public class QueryTable {
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
    public void selectTable1() {
        try {
            statement = connection.createStatement();
            String sql = "SELECT DOMAIN, AVG(CORE) Average_CPU_Usage, AVG(DB) Average_DB_Usage \n" +
                    "FROM WEB_STAT \n" +
                    "GROUP BY DOMAIN \n" +
                    "ORDER BY DOMAIN DESC";
            ResultSet rs = statement.executeQuery(sql);

            while (rs.next()) {
                String domain = rs.getString("DOMAIN");
                System.out.print(domain + "\t");
                double average_cpu_usage = rs.getDouble("Average_CPU_Usage");
                System.out.print(average_cpu_usage + "\t");
                double average_db_usage = rs.getDouble("Average_DB_Usage");
                System.out.println(average_db_usage + "\t");
            }

            long costTime = System.currentTimeMillis() - startTime;
            log.info("costTime:" + costTime);
        } catch (SQLException e) {
            log.error("select table error!", e);
        }
    }

    @Test
    public void selectTable2() {
        try {
            statement = connection.createStatement();
            String sql = "SELECT TRUNC(DATE,'DAY') DAY, SUM(CORE) TOTAL_CPU_Usage, MIN(CORE) MIN_CPU_Usage, MAX(CORE) MAX_CPU_Usage \n" +
                    "FROM WEB_STAT \n" +
                    "WHERE DOMAIN LIKE 'Salesforce%' \n" +
                    "GROUP BY TRUNC(DATE,'DAY')";
            ResultSet rs = statement.executeQuery(sql);

            while (rs.next()) {
                Date day = rs.getDate("DAY");
                System.out.print(day + "\t");
                long total_cpu_usage = rs.getLong("TOTAL_CPU_Usage");
                System.out.print(total_cpu_usage + "\t");
                long min_cpu_usage = rs.getLong("MIN_CPU_Usage");
                System.out.print(min_cpu_usage + "\t");
                long max_cpu_usage = rs.getLong("MAX_CPU_Usage");
                System.out.println(max_cpu_usage + "\t");
            }

            long costTime = System.currentTimeMillis() - startTime;
            log.info("costTime:" + costTime);
        } catch (SQLException e) {
            log.error("select table error!", e);
        }
    }

    @Test
    public void selectTable3() {
        try {
            statement = connection.createStatement();
            String sql = "SELECT HOST, SUM(ACTIVE_VISITOR) TOTAL_ACTIVE_VISITORS \n" +
                    "FROM WEB_STAT\n" +
                    "WHERE DB > (CORE * 1)\n" +
                    "GROUP BY HOST";
            ResultSet rs = statement.executeQuery(sql);

            while (rs.next()) {
                String host = rs.getString("HOST");
                System.out.print(host + "\t");
                long total_active_visitors = rs.getLong("TOTAL_ACTIVE_VISITORS");
                System.out.println(total_active_visitors + "\t");
            }

            long costTime = System.currentTimeMillis() - startTime;
            log.info("costTime:" + costTime);
        } catch (SQLException e) {
            log.error("select table error!", e);
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
