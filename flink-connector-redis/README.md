# Flink Connector Redis

The Redis connector allows for reading data from and writing data into Redis.

## How to create a Redis Table

### string

```sql
CREATE TABLE RedisTable (
  `key`   STRING,
  `value` STRING
) WITH (
  'connector' = 'redis',
  'redis.host' = 'localhost',
  'redis.port' = '6379',
  'redis.data-type' = 'string',
  'redis.key-pattern' = '*',
)
```

### list/set

```sql
CREATE TABLE RedisTable (
  `key`   STRING,
  `value` ARRAY<STRING>
) WITH (
  'connector' = 'redis',
  'redis.host' = 'localhost',
  'redis.port' = '6379',
  'redis.data-type' = 'list', -- or 'set'
  'redis.key-pattern' = '*',
)
```

### sorted set

```sql
CREATE TABLE RedisTable (
  `key`   STRING,
  `value` ARRAY<ROW<score DOUBLE, elem STRING>>
) WITH (
  'connector' = 'redis',
  'redis.host' = 'localhost',
  'redis.port' = '6379',
  'redis.data-type' = 'sorted_set',
  'redis.key-pattern' = '*',
)
```

or

```sql
CREATE TABLE RedisTable (
  `key`   STRING,
  `score` DOUBLE,
  `elem`  STRING
) WITH (
  'connector' = 'redis',
  'redis.host' = 'localhost',
  'redis.port' = '6379',
  'redis.data-type' = 'sorted_set',
  'redis.key-pattern' = '*',
)
```

### hash

```sql
CREATE TABLE RedisTable (
  `key`   STRING,
  `value` MAP<STRING, STRING>
) WITH (
  'connector' = 'redis',
  'redis.host' = 'localhost',
  'redis.port' = '6379',
  'redis.data-type' = 'hash',
  'redis.key-pattern' = '*',
)
```

or

```sql
CREATE TABLE RedisTable (
  `key`   STRING,
  `field` STRING,
  `value` STRING
) WITH (
  'connector' = 'redis',
  'redis.host' = 'localhost',
  'redis.port' = '6379',
  'redis.data-type' = 'hash',
  'redis.key-pattern' = '*',
)
```

### pubsub

publish:

```sql
CREATE TABLE RedisTable_publish (
  `id`   INT,
  `name` STRING,
  `age`  INT
) WITH (
  'connector' = 'redis',
  'redis.host' = 'localhost',
  'redis.port' = '6379',
  'redis.data-type' = 'pubsub',
  'redis.pubsub.publish-channel' = 'test',
  'format' = 'csv'
)
```

subscribe:

```sql
CREATE TABLE RedisTable_publish (
  `id`   INT,
  `name` STRING,
  `age`  INT
) WITH (
  'connector' = 'redis',
  'redis.host' = 'localhost',
  'redis.port' = '6379',
  'redis.data-type' = 'pubsub',
  'redis.pubsub.subscribe-patterns' = 'test',
  'format' = 'csv'
)
```

## Connector Options

|Option | Required | Default|Type | Description |
|:-----|:---------|:-------|:-----|:------------|
|connector|required|(none)|String|Specify what connector to use, for Redis use: 'redis'.|
|redis.deploy-mode|required|single|enum|Deploy mode for Redis, other options: 'sentinel', 'cluster', 'sharded'.|

## UDF

## Data Type Mapping
