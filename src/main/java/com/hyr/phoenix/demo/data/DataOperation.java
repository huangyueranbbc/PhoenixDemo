package com.hyr.phoenix.demo.data;

import com.hyr.phoenix.demo.Conf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/*******************************************************************************
 * @date 2018-08-07 上午 11:16
 * @author: huangyueran
 * @Description: phoenixSQL批量插入数据
 ******************************************************************************/
public class DataOperation {
    private final Logger log = LoggerFactory.getLogger(Conf.class);

    private Connection connection = null;
    private PreparedStatement preparedStatement = null;
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

    /**
     * phoenix 将insert和update操作合在一起
     */
    @Test
    public void insertAndUpdateBatchTable() {
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

    /**
     * phoenix 删除数据
     */
    @Test
    public void deleteData() {
        try {
            String sql = "DELETE FROM WEB_STAT WHERE HOST = 'EU'";
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            int result = statement.executeUpdate(sql);

            connection.commit();

            long costTime = System.currentTimeMillis() - startTime;
            log.info("delete data success ,result:{} ,costTime:{}", result, costTime);
        } catch (Exception e) {
            log.error("delete data error!", e);
        }

    }

    private List<String> readDataSource(String fileName) {
        List<String> dataSource = new ArrayList<String>();

        BufferedReader reader = null;

        try {
            File file = new File(this.getClass().getClassLoader().getResource(fileName).getPath());
            reader = new BufferedReader(new FileReader(file));

            String line = null;

            while ((line = reader.readLine()) != null) {
                dataSource.add(line);
            }
        } catch (Exception e) {
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
