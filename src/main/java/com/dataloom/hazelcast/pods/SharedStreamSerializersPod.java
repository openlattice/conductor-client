package com.dataloom.hazelcast.pods;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.stereotype.Component;

import com.kryptnostic.conductor.rpc.serializers.SharedStreamSerializers;

@Configuration
@ComponentScan(
    basePackageClasses = { SharedStreamSerializers.class },
    includeFilters = @ComponentScan.Filter(
        value = { Component.class },
        type = FilterType.ANNOTATION ) )
public class SharedStreamSerializersPod {}
