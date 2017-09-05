package com.dataloom.linking.util;

import java.util.Map;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.linking.Entity;

public interface MetricExtractor {
    double extract( Entity lhs, Entity rhs, Map<FullQualifiedName, String> fqnToIdMap );
}