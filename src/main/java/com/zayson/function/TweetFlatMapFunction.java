package com.zayson.function;

import com.zayson.model.EntitySentiment;
import com.zayson.model.Tweet;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.shaded.guava30.com.google.common.base.Splitter;
import org.apache.flink.util.Collector;

import java.util.concurrent.ThreadLocalRandom;

public class TweetFlatMapFunction extends RichFlatMapFunction<Tweet, EntitySentiment> {
    private transient Counter counter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("flatmap-counter");
    }

    @Override
    public void flatMap(Tweet tweet, Collector<EntitySentiment> out) throws Exception {
        Iterable<String> splitTweet = Splitter.on(' ').split(tweet.getText().toLowerCase());

        for (String entity : splitTweet) {
            EntitySentiment entitySentiment = EntitySentiment.builder()
                    .id(tweet.getId())
                    .text(tweet.getText())
                    .source(tweet.getSource())
                    .sentimentMagnitude(randomDoubleScore())
                    .salience(randomDoubleScore())
                    .sentimentScore(randomDoubleScore())
                    .entity(entity)
                    .createdAt(tweet.getCreatedAt())
                    .modifiedAt(tweet.getModifiedAt())
                    .build();

            counter.inc();
            out.collect(entitySentiment);
        }
    }

    private Double randomDoubleScore() {
        return ThreadLocalRandom.current().nextDouble(0, 1);
    }
}
