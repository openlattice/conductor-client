package com.kryptnostic.conductor.rpc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.rhizome.configuration.Configuration;
import com.kryptnostic.rhizome.configuration.ConfigurationKey;
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey;

public class ConductorConfiguration implements Configuration {
    private static final long             serialVersionUID           = -3847142110887587615L;
    private static final ConfigurationKey key                        = new SimpleConfigurationKey( "conductor.yaml" );

    private static final String           REPORT_EMAIL_ADDRESS_FIELD = "reportEmailAddress";

    private final String                  reportEmailAddress;

    @JsonCreator
    public ConductorConfiguration(
            @JsonProperty( REPORT_EMAIL_ADDRESS_FIELD ) String reportEmailAddress) {
        this.reportEmailAddress = reportEmailAddress;
    }

    @JsonProperty( REPORT_EMAIL_ADDRESS_FIELD )
    public String getReportEmailAddress() {
        return reportEmailAddress;
    }

    @Override
    @JsonIgnore
    public ConfigurationKey getKey() {
        return key;
    }

    @JsonIgnore
    public static ConfigurationKey key() {
        return key;
    }

}
