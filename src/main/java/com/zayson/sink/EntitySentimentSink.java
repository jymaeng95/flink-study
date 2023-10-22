package com.zayson.sink;

import com.zayson.model.EntitySentiment;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class EntitySentimentSink {

    private static final String SINK_PATH = "/Users/zayson/Desktop/Zayson Study/flink/flink-study/entity.txt";

    public static FileSink<EntitySentiment> createEntitySentimentSink() {
        return FileSink
                .forRowFormat(new Path(SINK_PATH), new SimpleStringEncoder<EntitySentiment>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(15))    // 15분이 지나는 경우
                                .withInactivityInterval(Duration.ofMinutes(5))    //  지난 5분동안 데이터가 스트리밍 되지 않은 경우
                                .withMaxPartSize(MemorySize.ofMebiBytes(1024))  // 마지막 레코드 작성 후 1GB가 넘는 경우
                                .build()
                )
                .build();
    }
}
