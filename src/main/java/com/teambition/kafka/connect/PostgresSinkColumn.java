package com.teambition.kafka.connect;

import org.apache.kafka.connect.errors.ConnectException;
import org.json.JSONObject;

import java.nio.charset.Charset;

/**
 * Created by Orange on 21/10/2016.
 */
public class PostgresSinkColumn {
  private String jsonPath = "";
  private String column = "";
  private int lengthLimit = 1000;
  
  public PostgresSinkColumn(String columnString) {
    String[] tmp = columnString.split(":");
    if (tmp.length == 1) {
      column = tmp[0];
      jsonPath = tmp[0];
    } else if (tmp.length == 2) {
      column = tmp[0];
      jsonPath = tmp[1];
    } else {
      throw new ConnectException("column parse error: " + columnString);
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
    if (string.getBytes().length > lengthLimit) {
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
