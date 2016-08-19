package com.kryptnostic.conductor.rpc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.rhizome.configuration.Configuration;
import com.kryptnostic.rhizome.configuration.ConfigurationKey;
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

public class ConductorConfiguration implements Configuration {
    private static final long             serialVersionUID           = -3847142110887587615L;
    private static final ConfigurationKey key                        = new SimpleConfigurationKey( "conductor.yaml" );

    private static final String           REPORT_EMAIL_ADDRESS_FIELD = "reportEmailAddress";
    private static final String           SPARK_JARS_FIELD = "sparkJars";

    private final String                  reportEmailAddress;
    private final String[] sparkJars;
    
    @JsonCreator
    public ConductorConfiguration(
            @JsonProperty( REPORT_EMAIL_ADDRESS_FIELD ) String reportEmailAddress,
            @JsonProperty( SPARK_JARS_FIELD ) List<String> sparkJars ) {
        this.reportEmailAddress = reportEmailAddress;
        //Filter out files that don't exist
        this.sparkJars = sparkJars.stream().filter( fp -> new File(fp).exists() ).collect(Collectors.toList()).toArray( new String[]{} );
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

    public String[] getSparkJars() {
        return sparkJars;
    }
}
