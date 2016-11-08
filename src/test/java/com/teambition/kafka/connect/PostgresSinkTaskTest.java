package com.teambition.kafka.connect;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.Test;

import java.sql.*;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class PostgresSinkTaskTest {
  
  public Connection connection;
  
  public static void main(String[] argv) throws SQLException, ClassNotFoundException {
    PostgresSinkTaskTest self = new PostgresSinkTaskTest();
    self.putPostgresqlDataTest();
  }
  
  @Test
  public void putPostgresqlDataTest() throws SQLException, ClassNotFoundException {
    clearTable("test_table");
    Map props = new HashMap<String, String>();
    props.put("db.connect", "postgresql://localhost:5432/travis_ci_test");
    props.put("db.user", "postgres");
    props.put("db.password", "");
    props.put("db.table", "test_table");
    props.put("db.columns", "ss,dd,dt");
    PostgresSinkTask task = new PostgresSinkTask();
    task.start(props);
  
    JSONObject json = new JSONObject();
    json.put("ss", "sample_string");
    json.put("dd", 1024);
    json.put("dt", "2016-10-30 12:34:56");
    SinkRecord record = new SinkRecord("fake_topic", 0, null, "", null, json.toMap(), 0);
    Collection<SinkRecord> collections = new ArrayList<>();
    collections.add(record);
    task.put(collections);
    task.flush(new HashMap<>());
    
    verifyData("test_table", json);
  }
  
  @Test
  public void postgresqlColumnInitTest() {
    PostgresSinkColumn column = new PostgresSinkColumn("ss");
    JSONObject json = new JSONObject();
    json.put("ss", "very very long very very long very very long very very long very very longvery very long very very long very very long very very long very very long very very long very very long very very long very very long very very long vvery very long very very long very very long very very long very very long very very long very very long very very long very very long very very longvery very long very very long very very long very very long very very long very very long very very long very very long very very long very very long vvery very long very very long very very long very very long very very long");
    System.out.println(column.getPostgresValue(json));
  }
  
  public void clearTable(String table) throws SQLException, ClassNotFoundException {
    Statement stmt = setUpConnection().createStatement();
    stmt.execute("CREATE TABLE " + table + "(id SERIAL PRIMARY KEY,ss VARCHAR(20) NULL,dd INT NULL,dt TIMESTAMP NULL)");
    stmt = setUpConnection().createStatement();
    stmt.execute("TRUNCATE " + table);
  }
  
  public Connection setUpConnection() throws ClassNotFoundException, SQLException {
    if (connection != null && !connection.isClosed()) return connection;
    String connectionString = "jdbc:postgresql://localhost:5432/travis_ci_test";
    String dbuser = "postgres";
    String dbpassword = "";
    Class.forName("org.postgresql.Driver");
    try {
      connection = DriverManager.getConnection(connectionString, dbuser, dbpassword);
    } catch (SQLException ex) {
      System.out.println("Is postgresql running?");
      System.out.println("Just run: ./scripts/start_server.sh ");
      throw ex;
    }
    return connection;
  }
  
  public void verifyData(String table, JSONObject json) throws ClassNotFoundException, SQLException {
    Statement stmt = setUpConnection().createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * from " + table);
    while (rs.next()) {
      Iterator<String> keys = json.keys();
      while (keys.hasNext()) {
        String key = keys.next();
        String sqlValue = rs.getString(key);
        String jsonValue = json.optString(key);
        assertEquals(jsonValue, sqlValue);
      }
    }
  }
}
