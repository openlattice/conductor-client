package com.kryptnostic.conductor.rpc;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

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
    private static final String           SPARK_JARS_FIELD           = "sparkJars";
    private static final String           SPARK_MASTERS_FIELD        = "sparkMasters";
    private static final String			  SEARCH_CONFIGURATION_FIELD = "searchConfiguration";

    private final String                  reportEmailAddress;
    private final List<String>            sparkMasters;
    private final String[]                sparkJars;
    private final SearchConfiguration     searchConfiguration;

    @JsonCreator
    public ConductorConfiguration(
            @JsonProperty( REPORT_EMAIL_ADDRESS_FIELD ) String reportEmailAddress,
            @JsonProperty( SPARK_MASTERS_FIELD ) List<String> sparkMasters,
            @JsonProperty( SPARK_JARS_FIELD ) List<String> sparkJars,
            @JsonProperty( SEARCH_CONFIGURATION_FIELD ) SearchConfiguration searchConfiguration ) {
        this.reportEmailAddress = reportEmailAddress;
        // Filter out files that don't exist
        this.sparkMasters = sparkMasters;
        this.sparkJars = sparkJars.stream().filter( fp -> new File( fp ).exists() ).collect( Collectors.toList() )
                .toArray( new String[] {} );
        this.searchConfiguration = searchConfiguration;
    }

    @JsonProperty( SPARK_MASTERS_FIELD )
    public List<String> getSparkMasters() {
        return sparkMasters;
    }

    @JsonProperty( REPORT_EMAIL_ADDRESS_FIELD )
    public String getReportEmailAddress() {
        return reportEmailAddress;
    }
    
    @JsonProperty( SEARCH_CONFIGURATION_FIELD )
    public SearchConfiguration getSearchConfiguration() {
    	return searchConfiguration;
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
