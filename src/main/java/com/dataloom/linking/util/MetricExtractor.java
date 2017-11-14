package com.dataloom.linking.util;

import java.util.Map;
import java.util.UUID;

import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

public interface MetricExtractor {
    double extract( Map<UUID, DelegatedStringSet> lhs, Map<UUID, DelegatedStringSet> rhs, Map<FullQualifiedName, UUID> fqnToIdMap );
}