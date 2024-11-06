# Mongo & Debezium outbox pattern tester

## Set up

Add the following line to your /etc/hosts file:

127.0.0.1 mongo

## Example working Debezium config which has passed the test

```json
{
  "tasks.max": "1",
  "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
  "mongodb.connection.string": "mongodb://mongo:27017/testdb?replicaSet=rs0",
  "key.converter.schemas.enable": "false",
  "value.converter.schemas.enable": "false",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "topic.prefix": "prefix",
  "sanitize.field.names": "true",
  "database.include.list": "testdb",
  "collection.include.list": "testdb.event_outbox",
  "skipped.operations": "d",
  "transforms": "outbox",
  "transforms.outbox.type": "io.debezium.connector.mongodb.transforms.outbox.MongoEventRouter",
  "transforms.outbox.collection.expand.json.payload": "true",
  "transforms.outbox.collection.field.event.key": "aggregateId",
  "transforms.outbox.route.by.field": "aggregateType",
  "transforms.outbox.route.topic.regex": "(.*)",
  "transforms.outbox.route.topic.replacement": "event_outbox",
  "transforms.outbox.collection.fields.additional.placement": "aggregateType:envelope,aggregateId:envelope,eventType:envelope,testAutomation:envelope,createdDate:envelope"
}
```