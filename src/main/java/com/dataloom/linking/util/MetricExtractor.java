package com.dataloom.linking.util;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.linking.Entity;

public interface MetricExtractor {
    double extract( Map<UUID, DelegatedStringSet> lhs, Map<UUID, DelegatedStringSet> rhs, Map<FullQualifiedName, UUID> fqnToIdMap );
}