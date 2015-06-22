package com.kryptnostic.instrumentation.v1.models;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.instrumentation.v1.constants.*;

public class MetricsRequest {
    private final String type;
    private final @Nullable String objectId;
    private final String requestBody;

    @JsonIgnore
    public MetricsRequest() {
        this( null, MetricsRequestMetadata.DEFAULT_TYPE, "" );
    }
    
    @JsonCreator
    public MetricsRequest(@JsonProperty( Names.OBJECT_BODY ) String requestBody ) {
        this( requestBody, MetricsRequestMetadata.DEFAULT_TYPE, "");
    }
    
    @JsonCreator
    public MetricsRequest(@JsonProperty( Names.OBJECT_BODY ) String requestBody, 
    		@JsonProperty( Names.TYPE_FIELD ) String type ) {
        this( requestBody, type, "");
    }

    @JsonCreator
    public MetricsRequest(@JsonProperty( Names.OBJECT_BODY ) String requestBody, 
    		@JsonProperty( Names.TYPE_FIELD ) String type,
            @JsonProperty ( Names.PARENT_OBJECT_ID_FIELD ) String objectId ) {
        this.type = type;
        this.objectId = objectId;
        this.requestBody = requestBody;
    }

    @JsonProperty( Names.PARENT_OBJECT_ID_FIELD )
    public String getObjectId() {
        return objectId;
    }

    @JsonIgnore
    public String getRequestBody() {
        return requestBody;
    }
    
    @JsonIgnore
    public String getType() {
        return type;
    }

}
