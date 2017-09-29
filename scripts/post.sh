curl -XPOST dev22:8088/connectors --header "Content-Type: application/json" --data '{
  "name": "postgresql-sink-connector",
  "config": {
    "connector.class": "com.teambition.kafka.connect.PostgresSinkConnector",
    "tasks.max": "1",
    "topics": "core-actionlog-post2",
    "db.connect": "postgresql://192.168.0.22:5439/core",
    "db.user": "postgres",
    "db.password": "docker",
    "db.table": "core_actionlog",
    "db.columns": "time_dt:timestamp,action,content,ip,useragent,city,country,_userId,_projectId,_organizationId"
  }
}'
# postgres://postgres:docker@dev22:5439/core?sslmode=disable
