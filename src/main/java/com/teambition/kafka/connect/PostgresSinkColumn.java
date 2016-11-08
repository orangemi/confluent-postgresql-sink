package com.teambition.kafka.connect;

import org.apache.kafka.connect.errors.ConnectException;
import org.json.JSONObject;

import java.nio.charset.Charset;

public class PostgresSinkColumn {
  private String jsonPath = "";
  private String column = "";
  private int lengthLimit = -1;
  
  public PostgresSinkColumn(String columnString) {
    String[] tmp = columnString.split(":");
    if (tmp.length == 1) {
      column = jsonPath = tmp[0];
    } else if (tmp.length == 2) {
      column = tmp[0];
      jsonPath = tmp[1];
    } else {
      throw new ConnectException("column parse error: " + columnString);
    }
    
    if (column.contains("(") && column.indexOf("(") < column.indexOf(")")) {
      String lengthLimitString = columnString.substring(column.indexOf("(") + 1, column.indexOf(")"));
      column = jsonPath = column.substring(0, column.indexOf("("));
      try {
        lengthLimit = Integer.valueOf(lengthLimitString);
      } catch (NumberFormatException ex) {
        throw new ConnectException(("column limit error: " + lengthLimitString), ex);
      }
    }
  }
  
  public String getPostgresValue(JSONObject json) {
    String[] path = jsonPath.split("\\.");
    for (int i = 0; i < path.length - 1; i++) {
      json = json.optJSONObject(path[i]);
    }
    String result = json.optString(path[path.length - 1]);
    return "'" + escape(result) + "'";
  }
  
  private String escape(String string) {
    if (lengthLimit >= 0 && string.getBytes().length > lengthLimit) {
      string = new String(string.getBytes(), 0, lengthLimit, Charset.forName("UTF-8"));
      // ignore trailling unrecognized bytes.
      if (string.getBytes().length > lengthLimit) {
        string = string.substring(0, string.length() - 1);
      }
    }
    string = string.replace("'", "''").replace("\\", "\\\\");
    return string;
  }
  
  public String getField() {
    return column;
  }
}
