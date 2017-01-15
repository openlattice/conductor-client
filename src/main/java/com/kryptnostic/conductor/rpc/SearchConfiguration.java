package com.kryptnostic.conductor.rpc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SearchConfiguration {
    private static final String ELASTICSEARCH_URL		= "elasticsearchUrl";
    private static final String ELASTICSEARCH_CLUSTER = "elasticsearchCluster";
    private static final String ELASTICSEARCH_PORT    = "elasticsearchPort";

    private final String        elasticsearchUrl;
    private final String        elasticsearchCluster;
    private final int           elasticsearchPort;

    @JsonCreator
    public SearchConfiguration(
            @JsonProperty( ELASTICSEARCH_URL ) String elasticsearchUrl,
            @JsonProperty( ELASTICSEARCH_CLUSTER ) String elasticsearchCluster,
            @JsonProperty( ELASTICSEARCH_PORT ) int elasticsearchPort ) {
        this.elasticsearchUrl = elasticsearchUrl;
        this.elasticsearchCluster = elasticsearchCluster;
        this.elasticsearchPort = elasticsearchPort;
    }

    @JsonProperty( ELASTICSEARCH_URL )
    public String getElasticsearchUrl() {
        return elasticsearchUrl;
    }

    @JsonProperty( ELASTICSEARCH_CLUSTER )
    public String getElasticsearchCluster() {
        return elasticsearchCluster;
    }
    
    @JsonProperty( ELASTICSEARCH_PORT )
    public int getElasticsearchPort() {
    	return elasticsearchPort;
    }
}
