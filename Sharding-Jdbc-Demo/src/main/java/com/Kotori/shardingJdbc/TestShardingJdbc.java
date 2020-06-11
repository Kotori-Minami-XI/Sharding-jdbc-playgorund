package com.Kotori.shardingJdbc;

import io.shardingjdbc.core.api.ShardingDataSourceFactory;
import io.shardingjdbc.core.api.config.ShardingRuleConfiguration;
import io.shardingjdbc.core.api.config.TableRuleConfiguration;
import io.shardingjdbc.core.api.config.strategy.InlineShardingStrategyConfiguration;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class TestShardingJdbc {

    @Test
    public void testSharding() {
        // 配置真实数据源
        Map<String, DataSource> dataSourceMap = new HashMap();

        // 配置第一个数据源
        BasicDataSource dataSource0 = new BasicDataSource();
        dataSource0.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource0.setUrl("jdbc:mysql://localhost:3306/ss_db0?characterEncoding=utf-8&serverTimezone=UTC&rewriteBatchedStatements=true");
        dataSource0.setUsername("root");
        dataSource0.setPassword("19926172xz");
        dataSourceMap.put("ss_db0", dataSource0);

        // 配置第二个数据源
        BasicDataSource dataSource1 = new BasicDataSource();
        dataSource1.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource1.setUrl("jdbc:mysql://localhost:3306/ss_db1?characterEncoding=utf-8&serverTimezone=UTC&rewriteBatchedStatements=true");
        dataSource1.setUsername("root");
        dataSource1.setPassword("19926172xz");
        dataSourceMap.put("ss_db1", dataSource1);

        // 配置Order表规则
        TableRuleConfiguration orderTableRuleConfig = new TableRuleConfiguration();
        orderTableRuleConfig.setLogicTable("t_order"); //逻辑表
        orderTableRuleConfig.setActualDataNodes("ss_db${0..1}.t_order${0..1}"); //物理表

        // 配置分库 + 分表策略
        orderTableRuleConfig.setDatabaseShardingStrategyConfig(
                new InlineShardingStrategyConfiguration("user_id", "ss_db${user_id % 2}"));
        orderTableRuleConfig.setTableShardingStrategyConfig(
                new InlineShardingStrategyConfiguration("order_id", "t_order${order_id % 2}"));

        // 配置分片规则
        ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
        shardingRuleConfig.getTableRuleConfigs().add(orderTableRuleConfig);

        // 获取数据源对象
        try {
            DataSource dataSource =
                    ShardingDataSourceFactory.createDataSource(dataSourceMap, shardingRuleConfig, new ConcurrentHashMap<String, Object>(), new Properties());
            Connection connection = dataSource.getConnection();
            query(connection);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void dropTables() throws SQLException {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://localhost:3306/ss_db0?characterEncoding=utf-8&serverTimezone=UTC&rewriteBatchedStatements=true");
        dataSource.setUsername("root");
        dataSource.setPassword("19926172xz");

        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE IF EXISTS t_order");
    }

    private void insert(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        String sql = "INSERT INTO t_order (user_id,order_id) VALUES (1,1)";
        statement.execute(sql);
    }

    private void query(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        String sql = "SELECT * FROM t_order WHERE order_id=1";
        ResultSet rs = statement.executeQuery(sql);
        while(rs.next()){
            String key = rs.getString("order_id");
            System.out.println(key);
        }
    }

}
