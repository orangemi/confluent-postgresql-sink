curl -XPOST localhost:8083/connectors --header "Content-Type: application/json" --data '{
  "name": "postgresql-sink-connector",
  "config": {
    "connector.class": "com.teambition.kafka.connect.PostgresSinkConnector",
    "tasks.max": "1",
    "topics": "core.actionlog.post",
    "db.connect": "postgresql://192.168.0.124:5432/docker",
    "db.user": "docker",
    "db.password": "docker",
    "db.table": "core_actionlog",
    "db.columns": "time_dt:timestamp,action,content,ip,useragent,city,country,_userId,_projectId,_organizationId"
  }
}'
