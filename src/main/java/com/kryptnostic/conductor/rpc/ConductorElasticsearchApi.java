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

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.PropertyType;
import com.dataloom.organization.Organization;
import com.google.common.base.Optional;

public interface ConductorElasticsearchApi {
	
	final String ES_PROPERTIES = "properties";
	final String PARENT = "_parent";
	final String TYPE = "type";
	final String OBJECT = "object";
	final String NESTED = "nested";
	final String KEYWORD = "keyword";
	final String NUM_SHARDS = "index.number_of_shards";
	final String NUM_REPLICAS = "index.number_of_replicas";
	
	// index setup consts
    final String ENTITY_SET_DATA_MODEL = "entity_set_data_model";
    final String ENTITY_SET_TYPE = "entity_set";
    
    // organizations setup consts
    final String ORGANIZATIONS = "organizations";
    final String ORGANIZATION = "organization";
    final String ORGANIZATION_TYPE = "organizationType";
    final String ORGANIZATION_ID = "organizationId";

	final String SECURABLE_OBJECT_INDEX_PREFIX = "securable_object_";
	final String SECURABLE_OBJECT_ROW_TYPE = "securable_object_row";
	final String ACL_KEY = "aclKey";
	final String PROPERTY_TYPE_ID = "propertyTypeId";
	
	// entity set field consts
	final String TYPE_FIELD = "_type";
	final String ENTITY_SET = "entitySet";
	final String ENTITY_SET_ID = "entitySetId";
	final String PROPERTY_TYPES = "propertyTypes";
	final String ACLS = "acls";
	final String NAME = "name";
	final String TITLE = "title";
	final String DESCRIPTION = "description";
	final String ENTITY_TYPE_ID = "entityTypeId";
	final String ID = "id";

	Boolean initializeEntitySetDataModelIndex();
	
	Boolean initializeOrganizationIndex();
	
	Boolean saveEntitySetToElasticsearch( EntitySet entitySet, List<PropertyType> propertyTypes, Principal principal );
	
	Boolean deleteEntitySet( UUID entitySetId );
	
	List<Map<String, Object>> executeEntitySetDataModelKeywordSearch(
			Optional<String> optionalSearchTerm,
			Optional<UUID> optionalEntityType,
			Optional<Set<UUID>> optionalPropertyTypes,
			Set<Principal> principals );
	
	Boolean updateEntitySetPermissions( UUID entitySetId, Principal principal, Set<Permission> permissions );
	
	Boolean updateEntitySetMetadata( EntitySet entitySet );
	
	Boolean updatePropertyTypesInEntitySet( UUID entitySetId, List<PropertyType> newPropertyTypes );
	
	Boolean createOrganization( Organization organization, Principal principal );
	
	Boolean updateOrganizationPermissions( UUID organizationId, Principal principal, Set<Permission> permissions );
	
	Boolean deleteOrganization( UUID organizationId );
	
	List<Map<String, Object>> executeOrganizationSearch( String searchTerm, Set<Principal> principals );
	
	Boolean updateOrganization( UUID id, Optional<String> optionalTitle, Optional<String> optionalDescription );
		
	Boolean createEntityData( UUID entitySetId, String entityId, Map<UUID, String> propertyValues );
	    
    List<Map<String, Object>> executeEntitySetDataSearch( UUID entitySetId, String searchTerm, Set<UUID> authorizedPropertyTypes );
	
    List<Map<String, Object>> executeEntitySetDataSearchAcrossIndices( Set<UUID> entitySetIds, Map<UUID, String> fieldSearches, int size, boolean explain );
}
