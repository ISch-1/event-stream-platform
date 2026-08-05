package com.esp.sink.writer;

import java.util.Properties;

import com.clickhouse.jdbc.ClickHouseDataSource;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * Isolated ClickHouse DataSource. We don't use Spring Boot's autoconfigured
 * DataSource because we don't want JPA/Hibernate to see ClickHouse — none
 * of our JPA entities live there.
 */
@Configuration
@RequiredArgsConstructor
public class ClickHouseConfig {

    @Value("${esp.clickhouse.url}")
    private String url;

    @Value("${esp.clickhouse.user}")
    private String user;

    @Value("${esp.clickhouse.password}")
    private String password;

    @Bean(name = "clickhouseDataSource")
    public DataSource clickhouseDataSource() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("socket_timeout", "30000");
        return new ClickHouseDataSource(url, props);
    }

    @Bean(name = "clickhouseJdbc")
    public JdbcTemplate clickhouseJdbc(DataSource clickhouseDataSource) {
        JdbcTemplate t = new JdbcTemplate(clickhouseDataSource);
        t.setFetchSize(1000);
        return t;
    }
}
