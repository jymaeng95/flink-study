package com.zayson;

import com.zayson.function.TweetFilterFunction;
import com.zayson.function.TweetFlatMapFunction;
import com.zayson.function.TweetMapFunction;
import com.zayson.model.Tweet;
import com.zayson.sink.EntitySentimentSink;
import com.zayson.source.TweetSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamJob {
    public static void main(String[] args) throws Exception {
        // 실행 환경 세팅
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * Streaming Flow
         * Source -> Filter -> Map -> FlatMap -> Sink
         * Each Function Get Metrics
         */
        DataStreamSource<Tweet> tweetStream = env.fromSource(
                TweetSource.createTweetSource(),
                WatermarkStrategy.noWatermarks(),
                "Tweet Generator Source"
        );
        tweetStream.print("===========");

        // Prometheus Enable
        tweetStream.filter(new TweetFilterFunction())
                .map(new TweetMapFunction())
                .flatMap(new TweetFlatMapFunction())
                .sinkTo(EntitySentimentSink.createEntitySentimentSink());

        env.execute();
    }
}