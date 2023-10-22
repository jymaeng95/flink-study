package com.zayson.function;

import com.zayson.model.Tweet;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

public class TweetFilterFunction extends RichFilterFunction<Tweet> {
    private transient Counter filterCounter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.filterCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("filter-counter");
    }

    @Override
    public boolean filter(Tweet tweet) throws Exception {
        filterCounter.inc();
        return !tweet.isRetweet();
    }
}
