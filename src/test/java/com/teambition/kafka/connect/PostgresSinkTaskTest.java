package com.teambition.kafka.connect;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;

import java.util.*;

/**
 * Created by Orange on 21/10/2016.
 */
public class PostgresSinkTaskTest {
  public static void main(String[] argv) {
    Map props = new HashMap<String, String>();
    props.put("db.connect", "postgresql://192.168.0.124:5432/docker");
    props.put("db.user", "docker");
    props.put("db.password", "docker");
    props.put("db.table", "core_actionlog");
    props.put("db.columns", "time_dt:timestamp,action,content,ip,useragent,city,country,_userId,_projectId,_organizationId");
    PostgresSinkTask task = new PostgresSinkTask();
    task.start(props);
  
    JSONObject json = new JSONObject();
    json.put("String", "String");
    json.put("timestamp", "2016-10-21");
    json.put("_userId", "abc");
    SinkRecord record = new SinkRecord("core_actionlog", 0, null, "", null, json, 0);
    Collection<SinkRecord> collections = new ArrayList<SinkRecord>() {
    };
    collections.add(record);
    collections.add(record);
    task.put(collections);
    task.debugSql();
    task.flush(new HashMap<TopicPartition, OffsetAndMetadata>());
    
  }
}
