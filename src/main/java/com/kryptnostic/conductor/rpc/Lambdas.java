/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

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
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.organization.Organization;
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
    
    public static Function<ConductorSparkApi, Boolean> createOrganization( Organization organization, Principal principal ) {
        return (Function<ConductorSparkApi, Boolean> & Serializable) ( api ) -> api
                .createOrganization( organization, principal );
    }
    
    public static Function<ConductorSparkApi, List<Map<String, Object>>> executeOrganizationKeywordSearch( String searchTerm ) {
        Set<Principal> principals = Principals.getCurrentPrincipals();
        return (Function<ConductorSparkApi, List<Map<String, Object>>> & Serializable) ( api ) -> api
                .executeOrganizationKeywordSearch( searchTerm, principals ); 
    }
    
    public static Function<ConductorSparkApi, Boolean> updateOrganization( UUID id, Optional<String> optionalTitle, Optional<String> optionalDescription ) {
        return (Function<ConductorSparkApi, Boolean> & Serializable) ( api ) -> api
                .updateOrganization( id, optionalTitle, optionalDescription );
    }
    
    public static Function<ConductorSparkApi, Boolean> deleteOrganization( UUID organizationId ) {
        return (Function<ConductorSparkApi, Boolean> & Serializable) ( api ) -> api
                .deleteOrganization( organizationId );
    }
    
    public static Function<ConductorSparkApi, Boolean> updateOrganizationPermissions( UUID organizationId, Principal principal, Set<Permission> permissions ) {
        return (Function<ConductorSparkApi, Boolean> & Serializable) ( api ) -> api
                .updateOrganizationPermissions( organizationId, principal, permissions );
    }
  
    public static Function<ConductorSparkApi, Boolean> updateEntitySetMetadata( EntitySet entitySet ) {
        return (Function<ConductorSparkApi, Boolean> & Serializable) ( api ) -> api
                .updateEntitySetMetadata( entitySet );
    }

    public static Function<ConductorSparkApi, Boolean> updatePropertyTypesInEntitySet( UUID entitySetId, List<PropertyType> newPropertyTypes ) {
        return (Function<ConductorSparkApi, Boolean> & Serializable) ( api ) -> api
                .updatePropertyTypesInEntitySet( entitySetId, newPropertyTypes );
    }
}