package com.zayson.function;

import com.zayson.model.Tweet;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

import java.util.Objects;

public class TweetMapFunction extends RichMapFunction<Tweet, Tweet> {
    private transient Counter mapCounter;
    private static final String BASIC_SOURCE = "en";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.mapCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("map-counter");
    }

    @Override
    public Tweet map(Tweet tweet) throws Exception {
        mapCounter.inc();
        if(!Objects.equals(BASIC_SOURCE, tweet.getSource())) {
            tweet.setSource(BASIC_SOURCE);
        }

        return tweet;
    }
}
