package com.kryptnostic.conductor.rpc;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.PropertyType;
import com.google.common.base.Optional;

public interface ConductorElasticsearchApi {
	
	// index setup consts
	final String ENTITY_SET_DATA_MODEL = "entity_set_data_model";
	final String ENTITY_SET_TYPE = "entity_set";
	final String ES_PROPERTIES = "properties";
	final String PARENT = "_parent";
	final String TYPE = "type";
	final String OBJECT = "object";
	final String NESTED = "nested";
	final String KEYWORD = "keyword";
	final String NUM_SHARDS = "index.number_of_shards";
	final String NUM_REPLICAS = "index.number_of_replicas";
	
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

	void initializeEntitySetDataModelIndex();
	
	void saveEntitySetToElasticsearch( EntitySet entitySet, List<PropertyType> propertyTypes, Principal principal );
	
	Boolean deleteEntitySet( UUID entitySetId );
	
	List<Map<String, Object>> executeEntitySetDataModelKeywordSearch(
			String searchTerm,
			Optional<UUID> optionalEntityType,
			Optional<Set<UUID>> optionalPropertyTypes,
			Set<Principal> principals );
	
	Boolean updateEntitySetPermissions( UUID entitySetId, Principal principal, Set<Permission> permissions );
	
	void updatePropertyTypesInEntitySet( UUID entitySetId, Set<PropertyType> newPropertyTypes );
	
}
