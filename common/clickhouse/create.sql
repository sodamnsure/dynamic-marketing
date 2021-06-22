set
allow_experimental_map_type = 1;

-- 创建kafka引擎表
create table default.event_detail_kafka
(
    account        String,
    appId          String,
    appVersion     String,
    carrier        String,
    deviceId       String,
    deviceType     String,
    ip             String,
    latitude       Float32,
    longitude      Float32,
    netType        String,
    osName         String,
    osVersion      String,
    releaseChannel String,
    resolution     String,
    sessionId      String,
    timeStamp      UInt64,
    eventId        String,
    properties     Map(String, String)
) engine = Kafka('feelings:9092', 'ActionLog', 'group1', 'JSONEachRow');


-- 创建事件明细表
create table default.event_detail
(
    account        String,
    appId          String,
    appVersion     String,
    carrier        String,
    deviceId       String,
    deviceType     String,
    ip             String,
    latitude       Float32,
    longitude      Float32,
    netType        String,
    osName         String,
    osVersion      String,
    releaseChannel String,
    resolution     String,
    sessionId      String,
    timeStamp      UInt64,
    eventId        String,
    properties     Map(String, String),
    index          u (deviceId) type minmax granularity 3,
    index          t ( timeStamp) type minmax granularity 3
) engine = MergeTree()
order by (deviceId, timeStamp);


-- 创建物化视图
create
materialized view default.event_view to default.event_detail
as
select account,
       appId,
       appVersion,
       carrier,
       deviceId,
       deviceType,
       ip,
       latitude,
       longitude,
       netType,
       osName,
       osVersion,
       releaseChannel,
       resolution,
       sessionId,
       timeStamp,
       eventId,
       properties
from default.event_detail_kafka;