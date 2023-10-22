package com.zayson.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class EntitySentiment {
    private Long id;
    private String source;
    private String text;
    private Long createdAt;
    private Long modifiedAt;
    private String entity;
    private Double salience;
    private Double sentimentMagnitude;
    private Double sentimentScore;
}
