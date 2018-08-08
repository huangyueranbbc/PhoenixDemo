package com.hyr.phoenix.demo.index;

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
 * @Description: phoenix的索引操作
 ******************************************************************************/
public class PhoenixIndex {
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
    public void createIndex() {
        try {
            statement = connection.createStatement();
            String sql = "CREATE INDEX my_idx_db ON WEB_STAT(DB)";
            int result = statement.executeUpdate(sql);

            long costTime = System.currentTimeMillis() - startTime;
            log.info("create index result:{} ,costTime:{}", result, costTime);
        } catch (SQLException e) {
            log.error("create index error!", e);
        }
    }

    /**
     * 创建二级索引(cover index)
     */
    @Test
    public void createCoverIndex() {
        try {
            statement = connection.createStatement();
            String sql = "CREATE INDEX my_idx_db_core_activevisitor ON WEB_STAT(DB) INCLUDE(CORE,ACTIVE_VISITOR)";
            int result = statement.executeUpdate(sql);

            long costTime = System.currentTimeMillis() - startTime;
            log.info("create index result:{} ,costTime:{}", result, costTime);
        } catch (SQLException e) {
            log.error("create cover index error!", e);
        }
    }

    /**
     * 查询索引执行计划
     */
    @Test
    public void explainSelectIndex() {
        try {
            statement = connection.createStatement();
            // 查询二级索引，SELECT的字段必须包括在include的字段中
            String sql = "explain select CORE from WEB_STAT where DB > 300";
            ResultSet rs = statement.executeQuery(sql);

            while (rs.next()) {
                String string = rs.getString(1);
                System.out.println(string);
            }

            long costTime = System.currentTimeMillis() - startTime;
            log.info("costTime:{}", costTime);
        } catch (SQLException e) {
            log.error("select index error!", e);
        }
    }

    @Test
    public void dropIndex() {
        try {
            statement = connection.createStatement();
            String sql = "DROP INDEX my_idx_db_core_activevisitor ON WEB_STAT";
            int result = statement.executeUpdate(sql);

            long costTime = System.currentTimeMillis() - startTime;
            log.info("drop index result:{} ,costTime:{}", result, costTime);
        } catch (SQLException e) {
            log.error("drop index error!", e);
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
