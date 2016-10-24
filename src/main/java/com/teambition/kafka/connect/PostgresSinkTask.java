package com.teambition.kafka.connect;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.json.JSONException;
import org.json.JSONObject;

import javax.sql.ConnectionEvent;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by Orange on 21/10/2016.
 */
public class PostgresSinkTask extends SinkTask {
  
  private Connection connection;
  private String table;
  private String columnNamesString;
  private List<PostgresSinkColumn> columns = new ArrayList<PostgresSinkColumn>();
  private List<String> rows = new ArrayList<>();
  @Override
  public String version() {
    return PostgresSinkConfig.VERSION;
  }
  
  @Override
  public void start(Map<String, String> props) {
    // postgresql://" + host + ":" + port + "/" + dbname + "?charSet=UNICODE"
    String connectionString = "jdbc:" + props.get(PostgresSinkConfig.POSTGRES_CONNECTION_URL_CONFIG);
    String dbuser = props.get(PostgresSinkConfig.POSTGRES_CONNECTION_USER_CONFIG);
    String dbpassword = props.get(PostgresSinkConfig.POSTGRES_CONNECTION_PASSWORD_CONFIG);
    try {
      Class.forName("org.postgresql.Driver");
      connection = DriverManager.getConnection(connectionString, dbuser, dbpassword);
    } catch (ClassNotFoundException ex) {
      throw new ConnectException(ex);
    } catch (SQLException ex) {
      throw new ConnectException(ex);
    }
  
    table = props.get(PostgresSinkConfig.POSTGRES_CONNECTION_TABLE_CONFIG);
    String columnsString = props.get(PostgresSinkConfig.POSTGRES_CONNECTION_COLUMNS_CONFIG);
    for (String columnString: columnsString.split(",")) {
      columns.add(new PostgresSinkColumn(columnString));
    }
    List<String> columnNames = new ArrayList<>();
    for (PostgresSinkColumn column: columns) {
      columnNames.add(column.getField());
    }
    columnNamesString = String.join(",", columnNames);
  }
  
  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record: records) {
      try {
        JSONObject json = (JSONObject)record.value();
        List<String> values = new ArrayList<>();
        for (PostgresSinkColumn column: columns) {
          values.add(column.getPostgresValue(json));
        }
        rows.add("(" + String.join(",", values) + ")");
      } catch (JSONException ex) {
        continue;
      }
    }
  }
  
  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    Statement stmt;
    try {
      stmt = connection.createStatement();
      String sql = "INSERT INTO " + table + " (" + columnNamesString + ") VALUES " + String.join(",", rows);
      stmt.execute(sql);
    } catch (SQLException ex) {
      throw new ConnectException(ex);
    }
  }
  
  @Override
  public void stop() {
    
  }
  
  public void debugSql() {
    String sql = "INSERT INTO " + table + " (" + columnNamesString + ") VALUES " + String.join(",", rows);
    System.out.println(sql);
  }
}
