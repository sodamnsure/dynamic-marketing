set allow_experimental_map_type = 1;
create table default.event_detail_kafka (
                                            account String,
                                            appId String,
                                            appVersion String,
                                            carrier String,
                                            deviceId String,
                                            deviceType String,
                                            ip String,
                                            latitude Float32,
                                            longitude Float32,
                                            netType String,
                                            osName String,
                                            osVersion String,
                                            releaseChannel String,
                                            resolution String,
                                            sessionId String,
                                            timeStamp UInt64,
                                            eventId String,
                                            properties Map(String, String)
) engine = Kafka('47.94.223.241:9092', 'ActionLog', 'group1', 'JSONEachRow');


select *
from default.event_detail_kafka;