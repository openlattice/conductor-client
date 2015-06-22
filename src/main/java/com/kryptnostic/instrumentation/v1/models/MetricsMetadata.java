package com.kryptnostic.instrumentation.v1.models;

import java.util.Set;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.kryptnostic.instrumentation.v1.constants.*;


/**
 * Stores document identifier and document version
 *
 * @author sinaiman, rbuckheit
 */
@Immutable
public class MetricsMetadata {
    @JsonIgnore
    public static final String       DEFAULT_TYPE = "metrics";
    public static final int			 DEFAULT_VERSION = 1;
    protected final String           id;
    protected final int              version;
    protected final String			 logMessage;

    protected final DateTime         createdTime;

    protected final String           type;


    /**
     * constructs metadata with default values
     * @param id
     */
    @JsonIgnore
    public MetricsMetadata( String id ) {
        this(id, 0, "");
    }

    @JsonIgnore
    public MetricsMetadata(
            String id,
            int version,
            String logMessage ) {
        this( id, version, logMessage, DEFAULT_TYPE);
    }

    @JsonIgnore
    public MetricsMetadata(
            String id,
            int version,
            String logMessage,
            String type ) {
        this( id, version, logMessage, type, DateTime.now() );
    }
    
    @JsonIgnore
    public MetricsMetadata(
            String id,
            String logMessage,
            String type ) {
        this( id, DEFAULT_VERSION, logMessage, type, DateTime.now() );
    }


    @JsonCreator
    public MetricsMetadata(
            @JsonProperty( Names.ID_FIELD ) String id,
            @JsonProperty( Names.VERSION_FIELD ) int version,
            @JsonProperty( Names.LOG_MESSAGE_FIELD ) String logMessage,
            @JsonProperty( Names.TYPE_FIELD ) String type,
            @JsonProperty( Names.CREATED_TIME ) DateTime createdTime) {
        this.id = id;
        this.version = version;
        this.logMessage = logMessage;

        this.type = type.toLowerCase();

        this.createdTime = createdTime;
    }

    /**
     * @return Document identifier
     */
    @JsonProperty( Names.ID_FIELD )
    public String getId() {
        return id;
    }

    /**
     * @return Version of document
     */
    @JsonProperty( Names.VERSION_FIELD )
    public int getVersion() {
        return version;
    }


    @JsonProperty( Names.CREATED_TIME )
    public DateTime getCreatedTime() {
        return createdTime;
    }

    @JsonProperty( Names.TYPE_FIELD )
    public String getType() {
        return type;
    }
    
    @JsonProperty( Names.LOG_MESSAGE )
    public String getLogMessage() {
        return logMessage;
    }
}
