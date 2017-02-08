package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.Principals;
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
    
    public static Function<ConductorSparkApi, Boolean> submitEntitySetToElasticsearch( EntitySet entitySet, List<PropertyType> propertyTypes, Principal principal ) {
    	return (Function<ConductorSparkApi, Boolean> & Serializable) ( api ) -> api
    			.submitEntitySetToElasticsearch( entitySet, propertyTypes, principal );
    }
    
    public static Function<ConductorSparkApi, List<Map<String, Object>>> executeElasticsearchMetadataQuery(
    		Optional<String> optionalQuery,
			Optional<UUID> optionalEntityType,
			Optional<Set<UUID>> optionalPropertyTypes ) {
    	Set<Principal> principals = Principals.getCurrentPrincipals();
    	return (Function<ConductorSparkApi, List<Map<String, Object>>> & Serializable) ( api ) -> api
                .executeElasticsearchMetadataQuery( optionalQuery, optionalEntityType, optionalPropertyTypes, principals );
    }
    
    public static Function<ConductorSparkApi, Boolean> updateEntitySetPermissions( UUID entitySetId, Principal principal, Set<Permission> permissions ) {
    	return (Function<ConductorSparkApi, Boolean> & Serializable) ( api ) -> api
    			.updateEntitySetPermissions( entitySetId, principal, permissions );
    }
    
    public static Function<ConductorSparkApi, Boolean> deleteEntitySet( UUID entitySetId ) {
    	return (Function<ConductorSparkApi, Boolean> & Serializable) ( api ) -> api
    			.deleteEntitySet( entitySetId );
    }
}