package com.hyr.phoenix.demo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-08-07 上午 10:59 
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description:
 ******************************************************************************/
public class SelectTable {
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
    public void selectTable() {
        try {
            statement = connection.createStatement();
            String sql = "select *  from WEB_STAT";
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
