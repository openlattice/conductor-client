package com.kryptnostic.conductor.rpc;

import java.util.List;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

public interface ConductorSparkApi {

    QueryResult getAllEntitiesOfType( FullQualifiedName entityTypeFqn );

    QueryResult getAllEntitiesOfEntitySet( FullQualifiedName entityFqn, String entitySetName );

    /**
     * Return QueryResult of <b>UUID's ONLY</b> of all entities matching a Look Up Entities Request.
     * 
     * @param request A LookupEntitiesRequest object
     * @return QueryResult of UUID's matching the lookup request
     */
    QueryResult getFilterEntities( LookupEntitiesRequest request );

}
