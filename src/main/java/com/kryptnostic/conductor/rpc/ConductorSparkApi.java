package com.kryptnostic.conductor.rpc;

import java.util.List;
import java.util.UUID;

public interface ConductorSparkApi {
    // Multimap<FullQualifiedName, ?> getEntity( UUID userId, Map<FullQualifiedName, ?> key );
    // List<Entity> getEntitySet( UUID userId, EntitySet entitySet );

    List<UUID> lookupEntities( String keyspace, LoadEntitiesRequest entityKey );
}
