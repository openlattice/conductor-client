package com.kryptnostic.instrumentation.v1.models;

import java.io.Serializable;
import java.util.Arrays;

import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.kodex.v1.constants.Names;
import com.kryptnostic.kodex.v1.crypto.ciphers.BlockCiphertext;
import com.kryptnostic.kodex.v1.models.blocks.ChunkingStrategy;

public class MetricsObject implements Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    private DateTime               creationTime;
    private MetricsMetadata		   metadata;

    
    @JsonCreator
    public MetricsObject(
    		@JsonProperty( Names.METRICS_METADATA) MetricsMetadata metadata) {
        this(metadata, DateTime.now());
    }
    
    @JsonCreator
    public MetricsObject(
    		@JsonProperty( Names.METRICS_METADATA) MetricsMetadata metadata,
            @JsonProperty( Names.CREATED_TIME ) DateTime createdTime ) {
        this.creationTime = createdTime;
        this.metadata = metadata;
    }



    @JsonProperty( Names.CREATED_TIME )
    public DateTime getCreationTime() {
        return creationTime;
    }

    
   @JsonProperty( Names.METRICS_METADATA )
    public MetricsMetadata getMetadata() {
        return metadata;
    }



}
