package com.teambition.kafka.connect;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  
  private static Logger log = LoggerFactory.getLogger(PostgresSinkTask.class);
  
  private long flushCount = 0L;
  private long bufferedSize = 0L;
  private Connection connection;
  private String table;
  private String columnNamesString;
  private List<PostgresSinkColumn> columns = new ArrayList<PostgresSinkColumn>();
  
  private Statement stmt;

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
      stmt = connection.createStatement();
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
        Map<String, Object> map = (Map<String, Object>)record.value();
        JSONObject json = new JSONObject(map);
        json.put("offset", record.kafkaOffset());
        log.debug("json record: " + json.toString());
        
        List<String> values = new ArrayList<>();
        for (PostgresSinkColumn column: columns) {
          values.add(column.getPostgresValue(json));
        }
        String row = "(" + String.join(",", values) + ")";
        String sql = "INSERT INTO " + table + " (" + columnNamesString + ") VALUES " + "(" + String.join(",", values) + ")";
        stmt.addBatch(sql);
        bufferedSize++;
      } catch (JSONException ex) {
        ex.printStackTrace();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    
    if (bufferedSize >= 1000) {
      flushSql();
    }
  }
  
  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    flushSql();
    if (flushCount != 0) {
      log.info("Flush {} Insert SQLs", flushCount);
      flushCount = 0;
    }
  }
  
  private void flushSql() {
    if (bufferedSize <= 0) return;
    try {
      int[] updateds = stmt.executeBatch();
      long inserted = bufferedSize;
      for (int updated: updateds) {
        inserted -= updated;
      }
      log.debug("Run {}, Inserted: {} ", bufferedSize, inserted);
      flushCount += bufferedSize;
      stmt.clearBatch();
      stmt = connection.createStatement();
      bufferedSize = 0;
      
    } catch (SQLException ex) {
      throw new ConnectException(ex);
    }
  }
  
  @Override
  public void stop() {
    
  }
}
