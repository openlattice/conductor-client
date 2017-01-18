package com.dataloom.hazelcast.pods;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kryptnostic.conductor.rpc.serializers.EntitySetStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.EntityTypeStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.FullQualifiedNameStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.PropertyTypeStreamSerializer;

@Configuration
public class IMapObjectStreamSerializersPod {
    
    @Bean
    public EntitySetStreamSerializer esss() {
        return new EntitySetStreamSerializer();
    }

    @Bean
    public EntityTypeStreamSerializer etss() {
        return new EntityTypeStreamSerializer();
    }

    @Bean
    public PropertyTypeStreamSerializer ptss() {
        return new PropertyTypeStreamSerializer();
    }

    @Bean
    public FullQualifiedNameStreamSerializer fqnss() {
        return new FullQualifiedNameStreamSerializer();
    }
    
}
