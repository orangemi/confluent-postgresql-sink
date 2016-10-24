package com.teambition.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Orange on 21/10/2016.
 */
public class PostgresSinkConnector extends SinkConnector {
  
  private String connectionString;
  private String dbuser;
  private String dbpassword;
  private String table;
  private String columnsString;
  
  @Override
  public String version() {
    return PostgresSinkConfig.VERSION;
  }
  
  @Override
  public void start(Map<String, String> props) {
    System.out.println("Start Connector props: " + props);
    connectionString = props.get(PostgresSinkConfig.POSTGRES_CONNECTION_URL_CONFIG);
    dbuser = props.get(PostgresSinkConfig.POSTGRES_CONNECTION_USER_CONFIG);
    dbpassword = props.get(PostgresSinkConfig.POSTGRES_CONNECTION_PASSWORD_CONFIG);
    table = props.get(PostgresSinkConfig.POSTGRES_CONNECTION_TABLE_CONFIG);
    columnsString = props.get(PostgresSinkConfig.POSTGRES_CONNECTION_COLUMNS_CONFIG);
  }
  
  @Override
  public Class<? extends Task> taskClass() {
    return PostgresSinkTask.class;
  }
  
  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> config = new HashMap<>();
      config.put(PostgresSinkConfig.POSTGRES_CONNECTION_URL_CONFIG, connectionString);
      config.put(PostgresSinkConfig.POSTGRES_CONNECTION_USER_CONFIG, dbuser);
      config.put(PostgresSinkConfig.POSTGRES_CONNECTION_PASSWORD_CONFIG, dbpassword);
      config.put(PostgresSinkConfig.POSTGRES_CONNECTION_TABLE_CONFIG, table);
      config.put(PostgresSinkConfig.POSTGRES_CONNECTION_COLUMNS_CONFIG, columnsString);
      configs.add(config);
    }
    return configs;
  }
  
  @Override
  public void stop() {
    
  }
  
  @Override
  public ConfigDef config() {
    return PostgresSinkConfig.config;
  }
}
