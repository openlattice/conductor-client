package com.kryptnostic.conductor.rpc;

import java.util.List;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.kryptnostic.conductor.rpc.odata.EntitySet;

public interface ConductorSparkApi {
    // Multimap<FullQualifiedName, ?> getEntity( UUID userId, Map<FullQualifiedName, ?> key );
    // List<Entity> getEntitySet( UUID userId, EntitySet entitySet );

    List<UUID> lookupEntities( LookupEntitiesRequest entityKey );

    QueryResult loadAllEntitiesOfType( FullQualifiedName entityTypeFqn );

    QueryResult loadEntitySet( EntitySet setType );
    
    /**
     * Return QueryResult of <b>UUID's ONLY</b> of all entities matching a Look Up Entities Request.
     * @param request A LookupEntitiesRequest object
     * @return QueryResult of UUID's matching the lookup request
     */
    QueryResult filterEntities( LookupEntitiesRequest request);
}
