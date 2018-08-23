package com.nextjoy.retention.db;

import com.alibaba.druid.pool.DruidDataSource;
import com.nextjoy.retention.utils.PropertyUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created with IDEA by Zhoufy on 2018/5/11.
 *
 * @author Zhoufy
 */
public class ConnectionFactory {

    private static ConnectionFactory connectionFactory = new ConnectionFactory();

    public static ConnectionFactory getInstance() {
        return connectionFactory;
    }


    public DruidDataSource getDruidDataSource(String username, String password, String jdbcurl) {

        DruidDataSource datasource = new DruidDataSource();

        String driverClassName = PropertyUtils.getValue("nextjoy.datasource.driverClassName");
        int initialSize = Integer.parseInt(PropertyUtils.getValue("nextjoy.datasource.initialSize"));
        int minIdle = Integer.parseInt(PropertyUtils.getValue("nextjoy.datasource.minIdle"));
        int maxActive = Integer.parseInt(PropertyUtils.getValue("nextjoy.datasource.maxActive"));
        int maxWait = Integer.parseInt(PropertyUtils.getValue("nextjoy.datasource.maxWait"));
        int timeBetweenEvictionRunsMillis = Integer.parseInt(PropertyUtils.getValue("nextjoy.datasource.timeBetweenEvictionRunsMillis"));
        int minEvictableIdleTimeMillis = Integer.parseInt(PropertyUtils.getValue("nextjoy.datasource.minEvictableIdleTimeMillis"));
        String validationQuery = PropertyUtils.getValue("nextjoy.datasource.validationQuery");
        boolean testWhileIdle = Boolean.parseBoolean(PropertyUtils.getValue("nextjoy.datasource.testWhileIdle"));
        boolean testOnBorrow = Boolean.parseBoolean(PropertyUtils.getValue("nextjoy.datasource.testOnBorrow"));
        boolean testOnReturn = Boolean.parseBoolean(PropertyUtils.getValue("nextjoy.datasource.testOnReturn"));
        int maxOpenPreparedStatements = Integer.parseInt(PropertyUtils.getValue("nextjoy.datasource.maxOpenPreparedStatements"));
        boolean removeAbandoned = Boolean.parseBoolean(PropertyUtils.getValue("nextjoy.datasource.removeAbandoned"));
        int removeAbandonedTimeout = Integer.parseInt(PropertyUtils.getValue("nextjoy.datasource.removeAbandonedTimeout"));
        boolean logAbandoned = Boolean.parseBoolean(PropertyUtils.getValue("nextjoy.datasource.logAbandoned"));

        datasource.setDriverClassName(driverClassName);
        datasource.setInitialSize(initialSize);
        datasource.setMinIdle(minIdle);
        datasource.setMaxActive(maxActive);
        datasource.setMaxWait(maxWait);
        datasource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        datasource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        datasource.setValidationQuery(validationQuery);
        datasource.setTestWhileIdle(testWhileIdle);
        datasource.setTestOnBorrow(testOnBorrow);
        datasource.setTestOnReturn(testOnReturn);
        datasource.setMaxOpenPreparedStatements(maxOpenPreparedStatements);
        datasource.setRemoveAbandoned(removeAbandoned);
        datasource.setRemoveAbandonedTimeout(removeAbandonedTimeout);
        datasource.setLogAbandoned(logAbandoned);

        datasource.setUsername(username);
        datasource.setPassword(password);
        datasource.setUrl(jdbcurl);
        return datasource;
    }

    /**
     * 关闭数据库连接
     *
     * @param connection
     * @param prepareStatement
     * @param resultSet
     */
    public void closeConnection(Connection connection, PreparedStatement prepareStatement, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (prepareStatement != null) {
                prepareStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage() + "code = " + e.getErrorCode());
        }
    }
}
