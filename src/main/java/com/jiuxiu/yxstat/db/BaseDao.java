package com.jiuxiu.yxstat.db;

import com.alibaba.druid.pool.DruidDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA by Zhoufy on 2018/5/11.
 *
 * @author Zhoufy
 */
public class BaseDao {

    protected Logger log = LoggerFactory.getLogger(this.getClass());

    private DruidDataSource druidDataSource = null;

    /**
     * 执行新增和修改的数据库操作,不用处理返回的ResultSet结果集
     *
     * @param sql    sql语句
     * @param params 参数，若为日期，需要特别处理
     * @return 收影响行数
     */
    protected int executeSql(String sql, Object[] params) {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = druidDataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    if (params[i] != null) {
                        if (params[i] instanceof java.util.Date) {
                            preparedStatement.setTimestamp(i + 1, new Timestamp(((Date) params[i]).getTime()));
                        } else {
                            preparedStatement.setObject(i + 1, params[i]);
                        }
                    } else {
                        preparedStatement.setString(i + 1, "");
                    }
                }
            }
            return preparedStatement.executeUpdate();
        } catch (SQLException e) {
            log.error(e.getMessage() + "code = " + e.getErrorCode() + ",sql:" + sql + "params:" + Arrays.toString(params), e);
            return -1;
        } finally {
            ConnectionFactory.getInstance().closeConnection(connection, preparedStatement, resultSet);
//            ConnectionFactory.getInstance().closeDruidDataSource(getDruidDataSource());
        }
    }

    /**
     * 批量执行sql语句 paramsArr是个2维数组，第一维度表示各条记录，第二维度表示各条记录里的各个parameter值
     */
    protected int[] executeBatchSql(String sql, Object[][] paramsArr) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = druidDataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            if (paramsArr != null) {
                for (int s = 0; s < paramsArr.length; s++) {
                    Object[] params = paramsArr[s];
                    if (params != null) {
                        // 设置sql语句参数
                        for (int i = 0; i < params.length; i++) {
                            if (params[i] != null) {
                                if (params[i] instanceof java.util.Date) {
                                    preparedStatement.setTimestamp(i + 1, new Timestamp(((Date) params[i]).getTime()));
                                } else {
                                    preparedStatement.setObject(i + 1, params[i]);
                                }
                            } else {
                                preparedStatement.setString(i + 1, "");
                            }
                        }
                        preparedStatement.addBatch();
                    }
                }
            }
            return preparedStatement.executeBatch();
        } catch (SQLException e) {
            log.error(e.getMessage() + "code = " + e.getErrorCode() + ",sql:" + sql, e);
        } finally {
            ConnectionFactory.getInstance().closeConnection(connection, preparedStatement, resultSet);
//            ConnectionFactory.getInstance().closeDruidDataSource(getDruidDataSource());
        }
        return null;
    }

    /**
     * 批量执行不同的sql语句 不包含查询
     * executeBatchSql
     *
     * @param sql 多个sql语句的数组
     * @return
     * @time 2015年9月23日下午4:23:16
     * @packageName com.dl.ios6
     */
    protected int[] executeBatchSql(String[] sql) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Connection connection = null;
        Statement state;
        try {
            connection = druidDataSource.getConnection();
            if (sql != null && sql.length > 0) {
                boolean autoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                state = connection.createStatement();
                for (int i = 0; i < sql.length; i++) {
                    state.addBatch(sql[i]);
                }
                int[] j = state.executeBatch();
                connection.commit();
                connection.setAutoCommit(autoCommit);
                state.close();
                ConnectionFactory.getInstance().closeConnection(connection, preparedStatement, resultSet);
                return j;
            }
        } catch (SQLException e) {
            log.error(e.getMessage() + "code = " + e.getErrorCode() + ",sql:" + sql, e);
        } finally {
            ConnectionFactory.getInstance().closeConnection(connection, preparedStatement, resultSet);
//            ConnectionFactory.getInstance().closeDruidDataSource(getDruidDataSource());
        }
        return null;
    }


    /**
     * 批量执行sql语句 paramsArr是个2维数组，第一维度表示各条记录，第二维度表示各条记录里的各个parameter值
     */
    protected int[] executeBatchSql(String sql, List<Object[]> paramsList) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = druidDataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql);

            if (paramsList == null) {
                return null;
            }
            for (int i = 0; i < paramsList.size(); i++) {
                Object[] tObj = paramsList.get(i);
                if (tObj == null) {
                    continue;
                }
                for (int j = 0; j < tObj.length; j++) {
                    Object curObj = tObj[j];
                    if (curObj != null) {
                        if (curObj instanceof java.util.Date) {
                            preparedStatement.setTimestamp(j + 1, new Timestamp(((java.util.Date) curObj).getTime()));
                        } else {
                            preparedStatement.setObject(j + 1, curObj);
                        }
                    } else {
                        preparedStatement.setString(j + 1, null);
                    }
                }
                preparedStatement.addBatch();
            }
            return preparedStatement.executeBatch();
        } catch (SQLException e) {
            log.error(e.getMessage() + "code = " + e.getErrorCode() + ",sql:" + sql, e);
        } finally {
            ConnectionFactory.getInstance().closeConnection(connection, preparedStatement, resultSet);
//            ConnectionFactory.getInstance().closeDruidDataSource(getDruidDataSource());
        }
        return null;
    }

    /**
     * 执行sql操作，把sql和params结合成一个sql语句
     * 执行sql查询的结果集交给sqlExecute这个接口函数处理，处理后封装的对象放到List里
     */
    protected List<Map<String, Object>> queryForList(String sql, Object[] params) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = druidDataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            // 设置sql语句参数
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    preparedStatement.setObject(i + 1, params[i]);
                }
            }
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData md = resultSet.getMetaData();
            int columnCount = md.getColumnCount();
            List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
            Map<String, Object> rowData;
            while (resultSet.next()) {
                rowData = new HashMap<String, Object>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(md.getColumnLabel(i), resultSet.getString(i));
                }
                list.add(rowData);
            }
            return list;
        } catch (SQLException e) {
            log.error(e.getMessage() + "code = " + e.getErrorCode() + ",sql:" + sql + " ,params:" + Arrays.toString(params), e);
        } finally {
            ConnectionFactory.getInstance().closeConnection(connection, preparedStatement, resultSet);
//            ConnectionFactory.getInstance().closeDruidDataSource(getDruidDataSource());
        }
        return null;
    }


    protected List<Map<String, String>> queryForList(String sql) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = druidDataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData md = resultSet.getMetaData();
            int columnCount = md.getColumnCount();
            List<Map<String, String>> list = new ArrayList<Map<String, String>>();
            Map<String, String> rowData;
            while (resultSet.next()) {
                rowData = new HashMap<String, String>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(md.getColumnLabel(i), resultSet.getString(i));
                }
                list.add(rowData);
            }
            return list;
        } catch (SQLException e) {
            log.error(e.getMessage() + "code = " + e.getErrorCode() + ",sql:" + sql, e);
        } finally {
            ConnectionFactory.getInstance().closeConnection(connection, preparedStatement, resultSet);
//            ConnectionFactory.getInstance().closeDruidDataSource(getDruidDataSource());
        }
        return null;
    }


    protected Map<String, Object> queryForMap(String sql, Object[] params) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = druidDataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    preparedStatement.setObject(i + 1, params[i]);
                }
            }
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData md = resultSet.getMetaData();
            int columnCount = md.getColumnCount();
            Map<String, Object> rowData = null;
            while (resultSet.next()) {
                rowData = new HashMap<String, Object>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(md.getColumnLabel(i), resultSet.getObject(i));
                }
                break;
            }
            return rowData;
        } catch (SQLException e) {
            log.error(e.getMessage() + "code = " + e.getErrorCode() + ",sql:" + sql, e);
        } finally {
            ConnectionFactory.getInstance().closeConnection(connection, preparedStatement, resultSet);
//            ConnectionFactory.getInstance().closeDruidDataSource(getDruidDataSource());
        }
        return null;
    }


    protected Map<String, String> queryForMapStr(String sql, Object[] params) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = druidDataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    preparedStatement.setObject(i + 1, params[i]);
                }
            }
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData md = resultSet.getMetaData();
            int columnCount = md.getColumnCount();
            Map<String, String> rowData = null;
            while (resultSet.next()) {
                rowData = new HashMap<String, String>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(md.getColumnLabel(i), resultSet.getString(i));
                }
                break;
            }
            return rowData;
        } catch (SQLException e) {
            log.error(e.getMessage() + "code = " + e.getErrorCode() + ",sql:" + sql, e);
        } finally {
            ConnectionFactory.getInstance().closeConnection(connection, preparedStatement, resultSet);
//            ConnectionFactory.getInstance().closeDruidDataSource(getDruidDataSource());
        }
        return null;
    }

    protected DruidDataSource getDruidDataSource() {
        return druidDataSource;
    }

    protected void setDruidDataSource(DruidDataSource druidDataSource) {
        this.druidDataSource = druidDataSource;
    }
}
