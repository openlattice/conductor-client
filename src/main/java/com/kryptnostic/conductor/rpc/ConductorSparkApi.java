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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.data.requests.LookupEntitiesRequest;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.organization.Organization;
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
     * Return QueryResult of <b>UUID's ONLY</b> of all entities linking a Look Up Entities Request.
     * 
     * @param request A LookupEntitiesRequest object
     * @return QueryResult of UUID's linking the lookup request
     */
    QueryResult getFilterEntities( LookupEntitiesRequest request );

	Boolean submitEntitySetToElasticsearch( EntitySet entitySet, List<PropertyType> propertyTypes, Principal principal );
	
	Boolean deleteEntitySet( UUID entitySetId );
	
	List<Map<String, Object>> executeElasticsearchMetadataQuery(
			Optional<String> query,
			Optional<UUID> optionalEntityType,
			Optional<Set<UUID>> optionalPropertyTypes,
			Set<Principal> principals );
	
	Boolean updateEntitySetMetadata( EntitySet entitySet );

	Boolean updatePropertyTypesInEntitySet( UUID entitySetId, List<PropertyType> newPropertyTypes );
	
	Boolean updateEntitySetPermissions( UUID entitySetId, Principal principal, Set<Permission> permissions );
	
	Boolean createOrganization( Organization organization, Principal principal );
	
	List<Map<String, Object>> executeOrganizationKeywordSearch( String searchTerm, Set<Principal> principals );
	
	Boolean updateOrganization( UUID id, Optional<String> optionalTitle, Optional<String> optionalDescription );
	
	Boolean deleteOrganization( UUID organizationId );
	
	Boolean updateOrganizationPermissions( UUID organizationId, Principal principal, Set<Permission> permissions );

	Boolean createEntityData( UUID entitySetId, String entityId, Map<UUID, String> propertyValues );
	
	List<Map<String, Object>> executeEntitySetDataSearch( UUID entitySetId, String searchTerm, Set<UUID> authorizedPropertyTypes );
	
    List<Map<String, Object>> executeEntitySetDataSearchAcrossIndices( Set<UUID> entitySetIds, Map<UUID, String> fieldSearches, int size, boolean explain );

}
