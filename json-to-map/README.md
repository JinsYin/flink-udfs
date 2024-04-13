# JSON_TO_MAP

将 JSON 字符串转换为 Flink Table/SQL 的 MAP 数据类型，参考 STR_TO_MAP 。
无论 JSON 的值类型如何，MAP 的返回类型统一为 `MAP<STRING, STRING>`。

## 用法

```sql
CREATE TEMPORARY FUNCTION IF NOT EXISTS JSON_TO_MAP 
       AS 'cn.guruguru.flink.udf.scalar.JsonToMap' 
       USING JAR '<path>/<to>/json-to-map.jar';

SELECT JSON_TO_MAP(jsondata) FROM <table_or_view>;
```

对于没有 JSON 嵌套的场景，可以使用系统函数来代替：

```sql
SELECT STR_TO_MAP(regexp_replace(regexp_replace(regexp_replace(jsondata, '"', ''), '{', ''), '}', ''), ',', ':') 
FROM <table_or_view>;
```
