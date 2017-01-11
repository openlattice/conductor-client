package com.kryptnostic.datastore.cassandra;

import java.util.Arrays;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.rhizome.configuration.cassandra.TableDefSource;

@Configuration
public class CassandraTablesPod {
    @Bean
    public TableDefSource loomTables() {
        return () -> Arrays.asList( Tables.values() ).stream().map( Tables::asTableDef );
    }
}
