package com.kryptnostic.conductor.rpc;

import java.util.List;
import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.dataloom.authorization.requests.Principal;
import com.dataloom.data.requests.LookupEntitiesRequest;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.PropertyType;
import com.google.common.base.Optional;

public interface ConductorSparkApi {

    //Mainly for compatibility - read all properties of the entity type
    QueryResult getAllEntitiesOfType( FullQualifiedName entityTypeFqn );

    QueryResult getAllEntitiesOfType( FullQualifiedName entityTypeFqn, List<PropertyType> authorizedProperties );

    //Mainly for compatibility - read all properties of the entity set
    QueryResult getAllEntitiesOfEntitySet(
            FullQualifiedName entityFqn,
            String entitySetName );

    QueryResult getAllEntitiesOfEntitySet(
            FullQualifiedName entityFqn,
            String entitySetName,
            List<PropertyType> authorizedProperties );

    /**
     * Return QueryResult of <b>UUID's ONLY</b> of all entities matching a Look Up Entities Request.
     * 
     * @param request A LookupEntitiesRequest object
     * @return QueryResult of UUID's matching the lookup request
     */
    QueryResult getFilterEntities( LookupEntitiesRequest request );

	Boolean submitEntitySetToElasticsearch( EntitySet entitySet, Set<PropertyType> propertyTypes );

	Boolean submitEntitySetDataToElasticsearch( EntitySet entitySet, Dataset<Row> entitySetData );
	
	Object executeElasticsearchMetadataQuery(
			String query,
			Optional<FullQualifiedName> optionalEntityType,
			Optional<Set<FullQualifiedName>> optionalPropertyTypes,
			Set<Principal> principals );
}
