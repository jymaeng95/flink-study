package com.zayson.function;

import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.zayson.model.EntitySentiment;
import com.zayson.model.Tweet;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.shaded.guava30.com.google.common.base.Splitter;
import org.apache.flink.util.Collector;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class TweetFlatMapFunction extends RichFlatMapFunction<Tweet, EntitySentiment> {
    private transient Counter counter;
    private transient Histogram histogram;
    private transient Meter meter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.counter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("flink-study-metric-group")
                .counter("flatmap-counter");

        this.histogram = getRuntimeContext()
                .getMetricGroup()
                .addGroup("flink-study-metric-group")
                .histogram(
                        "study-histogram",
                        new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new SlidingTimeWindowArrayReservoir(1000, TimeUnit.MILLISECONDS)))
                );

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("flink-study-metric-group")
                .meter("study-meter", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
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

            counter.inc();  // Counter 증가
            out.collect(entitySentiment);
        }

        histogram.update(System.currentTimeMillis() - tweet.getTimestamp());
        meter.markEvent();
    }

    private Double randomDoubleScore() {
        return ThreadLocalRandom.current().nextDouble(0, 1);
    }
}
