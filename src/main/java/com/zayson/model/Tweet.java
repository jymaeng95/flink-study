package com.zayson.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Tweet {
    private String source;
    private Long id;
    private String text;
    private Long createdAt;
    private Long modifiedAt;
    private boolean isRetweet;
}
