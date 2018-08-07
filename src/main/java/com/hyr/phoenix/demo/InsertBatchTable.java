package com.hyr.phoenix.demo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-08-07 上午 11:16 
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: phoenixSQL批量插入数据
 ******************************************************************************/
public class InsertBatchTable {
    private final Logger log = LoggerFactory.getLogger(Conf.class);

    private Connection connection = null;
    private PreparedStatement preparedStatement = null;
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
    public void insertBatchTable() {
        try {
            String sql = "UPSERT INTO  WEB_STAT(HOST,DOMAIN,FEATURE,DATE,CORE,DB,ACTIVE_VISITOR) values(?,?,?,?,?,?,?)";
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(sql);

            List<String> results = readDataSource("WEB_STAT.csv");
            for (String result : results) {
                log.info("get result:" + result);
                String[] data = result.split(",");
                preparedStatement.setString(1, data[0]);
                preparedStatement.setString(2, data[1]);
                preparedStatement.setString(3, data[2]);
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = formatter.parse(data[3]);
                preparedStatement.setDate(4, new java.sql.Date(date.getTime()));
                preparedStatement.setLong(5, Long.parseLong(data[4]));
                preparedStatement.setLong(6, Long.parseLong(data[5]));
                preparedStatement.setInt(7, Integer.parseInt(data[6]));
                preparedStatement.addBatch();
            }

            int[] ids = preparedStatement.executeBatch();
            connection.commit();

            long costTime = System.currentTimeMillis() - startTime;
            log.info("insert table success ,ids:{} ,costTime:{}", ids, costTime);
        } catch (Exception e) {
            log.error("insert table error!", e);
        }

    }

    private List<String> readDataSource(String fileName) {
        File file = new File(this.getClass().getClassLoader().getResource(fileName).getPath());
        List<String> dataSource = new ArrayList<String>();

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));

            String line = null;

            while ((line = reader.readLine()) != null) {
                dataSource.add(line);
            }
        } catch (IOException e) {
            log.error("read file error!", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ex) {
                    log.error("reader close error!", ex);
                }
            }
        }
        return dataSource;
    }

    @After
    public void close() {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
                preparedStatement = null;
            } catch (SQLException e) {
                log.error("preparedStatement close error!", e);
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