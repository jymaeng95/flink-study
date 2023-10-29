package com.zayson.source;

import com.zayson.model.Tweet;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class TweetSource {
    private static final List<String> sources = List.of("en", "kr", "jp");
    private static final List<String> texts = List.of("bitcoin", "bitcoin ethereum", "ethereum");
    private static final List<Boolean> tweetStatus = List.of(true, false);

    private static final double RECORDS_PER_SECONDS = 3;
    public static DataGeneratorSource<Tweet> createUnboundedTweetSource() {
        GeneratorFunction<Long, Tweet> tweetGenerator = index -> Tweet.builder()
                .id(Instant.now().getEpochSecond())
                .source(randomData(sources))
                .text(randomData(texts))
                .createdAt(Instant.now().getEpochSecond())
                .modifiedAt(Instant.now().getEpochSecond())
                .isRetweet(randomData(tweetStatus))
                .build();

        return new DataGeneratorSource<>(
                tweetGenerator,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(RECORDS_PER_SECONDS),
                GenericTypeInfo.of(Tweet.class)
        );
    }

    public static List<Tweet> createBoundedTweetSource() {
        List<Tweet> boundedTweetList = new ArrayList<>();
        for (int size = 0; size < 100; size++) {
            boundedTweetList.add(
                    Tweet.builder()
                            .id(Instant.now().getEpochSecond())
                            .source(randomData(sources))
                            .text(randomData(texts))
                            .createdAt(Instant.now().getEpochSecond())
                            .modifiedAt(Instant.now().getEpochSecond())
                            .isRetweet(randomData(tweetStatus))
                            .build()
            );
        }

        return boundedTweetList;
    }

    private static <T> T randomData(List<T> targets) {
        return targets.get(ThreadLocalRandom.current().nextInt(0, targets.size()));
    }
}
