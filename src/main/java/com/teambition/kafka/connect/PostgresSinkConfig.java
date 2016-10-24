package com.teambition.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class PostgresSinkConfig extends AbstractConfig {
  
  public static final String VERSION = "0.1.0";
  
  public static final String POSTGRES_CONNECTION_URL_CONFIG = "db.connect";
  public static final String POSTGRES_CONNECTION_USER_CONFIG = "db.user";
  public static final String POSTGRES_CONNECTION_PASSWORD_CONFIG = "db.password";
  public static final String POSTGRES_CONNECTION_COLUMNS_CONFIG = "db.columns";
  public static final String POSTGRES_CONNECTION_TABLE_CONFIG = "db.table";
  public static final String POSTGRES_CONNECTION_URL_CONFIG_DOC = "db.connect";
  public static final String POSTGRES_CONNECTION_USER_CONFIG_DOC = "db.user";
  public static final String POSTGRES_CONNECTION_PASSWORD_CONFIG_DOC = "db.password";
  public static final String POSTGRES_CONNECTION_COLUMNS_CONFIG_DOC = "db.columns";
  public static final String POSTGRES_CONNECTION_TABLE_CONFIG_DOC = "db.table";

  public static final ConfigDef config = new ConfigDef()
    .define(
      POSTGRES_CONNECTION_URL_CONFIG,
      ConfigDef.Type.STRING,
      ConfigDef.Importance.HIGH,
      POSTGRES_CONNECTION_URL_CONFIG_DOC
    )
    .define(
      POSTGRES_CONNECTION_USER_CONFIG,
      ConfigDef.Type.STRING,
      ConfigDef.Importance.HIGH,
      POSTGRES_CONNECTION_USER_CONFIG_DOC
    )
    .define(
      POSTGRES_CONNECTION_PASSWORD_CONFIG,
      ConfigDef.Type.STRING,
      ConfigDef.Importance.HIGH,
      POSTGRES_CONNECTION_PASSWORD_CONFIG_DOC
    )
    .define(
      POSTGRES_CONNECTION_COLUMNS_CONFIG,
      ConfigDef.Type.STRING,
      ConfigDef.Importance.HIGH,
      POSTGRES_CONNECTION_COLUMNS_CONFIG_DOC
    )
    .define(
      POSTGRES_CONNECTION_TABLE_CONFIG,
      ConfigDef.Type.STRING,
      ConfigDef.Importance.HIGH,
      POSTGRES_CONNECTION_TABLE_CONFIG_DOC
    );
  
  public PostgresSinkConfig(Map<String, String> props) {
    super(PostgresSinkConfig.config, props);
    
  }
  
  public static void main(String... args) {
    System.out.println(config.toRst());
  }
}
