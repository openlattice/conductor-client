package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.dataloom.authorization.Principals;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.data.requests.LookupEntitiesRequest;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.PropertyType;
import com.google.common.base.Optional;

public class Lambdas implements Serializable {
    private static final long serialVersionUID = -8384320983731367620L;

    public static Runnable foo() {
        return (Runnable & Serializable) () -> System.out.println( "UNSTOPPABLE" );
    }

    public static Function<ConductorSparkApi, QueryResult> getAllEntitiesOfType( FullQualifiedName fqn, List<PropertyType> authorizedProperties ) {
        return (Function<ConductorSparkApi, QueryResult> & Serializable) ( api ) -> api.getAllEntitiesOfType( fqn, authorizedProperties );
    }

    public static Function<ConductorSparkApi, QueryResult> getFilteredEntities(
            LookupEntitiesRequest lookupEntitiesRequest ) {
        return (Function<ConductorSparkApi, QueryResult> & Serializable) ( api ) -> api
                .getFilterEntities( lookupEntitiesRequest );
    }

    public static Function<ConductorSparkApi, QueryResult> getAllEntitiesOfEntitySet(
            FullQualifiedName entityFqn,
            String entitySetName,
            List<PropertyType> authorizedProperties ) {
        return (Function<ConductorSparkApi, QueryResult> & Serializable) ( api ) -> api
                .getAllEntitiesOfEntitySet( entityFqn, entitySetName, authorizedProperties );
    }
    
    public static Function<ConductorSparkApi, Boolean> submitEntitySetToElasticsearch( EntitySet entitySet, Set<PropertyType> propertyTypes ) {
    	Set<Principal> principals = Principals.getCurrentPrincipals();
    	return (Function<ConductorSparkApi, Boolean> & Serializable) ( api ) -> api
    			.submitEntitySetToElasticsearch( entitySet, propertyTypes );
    }
    
    public static Function<ConductorSparkApi, Object> executeElasticsearchMetadataQuery(
    		String query,
			Optional<FullQualifiedName> optionalEntityType,
			Optional<Set<FullQualifiedName>> optionalPropertyTypes ) {
    	Set<Principal> principals = Principals.getCurrentPrincipals();
    	return (Function<ConductorSparkApi, Object> & Serializable) ( api ) -> api
                .executeElasticsearchMetadataQuery( query, optionalEntityType, optionalPropertyTypes, principals );
    }
}