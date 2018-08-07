package com.hyr.phoenix.demo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-08-07 上午 10:59 
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: phoenix的SQL创建表
 ******************************************************************************/
public class CreateTable {
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
    public void createTable() {
        try {
            statement = connection.createStatement();
            String sql = "CREATE TABLE IF NOT EXISTS WEB_STAT (\n" +
                    "     HOST CHAR(2) NOT NULL,\n" +
                    "     DOMAIN VARCHAR NOT NULL,\n" +
                    "     FEATURE VARCHAR NOT NULL,\n" +
                    "     DATE DATE NOT NULL,\n" +
                    "     USAGE.CORE BIGINT,\n" +
                    "     USAGE.DB BIGINT,\n" +
                    "     STATS.ACTIVE_VISITOR INTEGER\n" +
                    "     CONSTRAINT PK PRIMARY KEY (HOST, DOMAIN, FEATURE, DATE)\n" +
                    ")";
            int result = statement.executeUpdate(sql);
            log.info("result:" + result);

            long costTime = System.currentTimeMillis() - startTime;
            log.info("costTime:" + costTime);
        } catch (SQLException e) {
            log.error("create table error!", e);
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
