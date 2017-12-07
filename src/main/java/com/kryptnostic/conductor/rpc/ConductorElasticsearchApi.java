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

import com.dataloom.authorization.Principal;
import com.dataloom.data.EntityKey;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.AssociationType;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.organization.Organization;
import com.dataloom.search.requests.SearchDetails;
import com.dataloom.search.requests.SearchResult;
import com.google.common.base.Optional;
import com.openlattice.authorization.AclKey;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface ConductorElasticsearchApi {

    // settings consts
    final String NUM_SHARDS         = "number_of_shards";
    final String NUM_REPLICAS       = "number_of_replicas";
    final String ANALYSIS           = "analysis";
    final String FILTER             = "filter";
    final String ANALYZER           = "analyzer";
    final String ENCODER            = "encoder";
    final String REPLACE            = "replace";
    final String TOKENIZER          = "tokenizer";
    final String STANDARD           = "standard";
    final String LOWERCASE          = "lowercase";
    final String PHONETIC           = "phonetic";
    final String METAPHONE          = "metaphone";
    final String METAPHONE_FILTER   = "metaphone_filter";
    final String METAPHONE_ANALYZER = "MetaphoneAnalyzer";

    final String ES_PROPERTIES = "properties";
    final String PARENT        = "_parent";
    final String TYPE          = "type";
    final String OBJECT        = "object";
    final String NESTED        = "nested";
    final String INDEX         = "index";
    final String NOT_ANALYZED  = "not_analyzed";

    // datatypes
    final String TEXT      = "text";
    final String KEYWORD   = "keyword";
    final String INTEGER   = "integer";
    final String SHORT     = "short";
    final String LONG      = "long";
    final String DOUBLE    = "double";
    final String FLOAT     = "float";
    final String BYTE      = "byte";
    final String DATE      = "date";
    final String BOOLEAN   = "boolean";
    final String BINARY    = "binary";
    final String GEO_POINT = "geo_point";

    // entity_set_data_model setup consts
    final String ENTITY_SET_DATA_MODEL = "entity_set_data_model";
    final String ENTITY_SET_TYPE       = "entity_set";

    // organizations setup consts
    final String ORGANIZATIONS     = "organizations";
    final String ORGANIZATION      = "organization";
    final String ORGANIZATION_TYPE = "organizationType";
    final String ORGANIZATION_ID   = "organizationId";

    final String SECURABLE_OBJECT_INDEX_PREFIX = "securable_object_";
    final String SECURABLE_OBJECT_TYPE_PREFIX  = "type_";
    final String ACL_KEY                       = "aclKey";
    final String PROPERTY_TYPE_ID              = "propertyTypeId";

    // entity_type_index setup consts
    final String ENTITY_TYPE_INDEX = "entity_type_index";
    final String ENTITY_TYPE       = "entity_type";

    // property_type_index setup consts
    final String PROPERTY_TYPE_INDEX = "property_type_index";
    final String PROPERTY_TYPE       = "property_type";

    // association_type_index setup consts
    final String ASSOCIATION_TYPE_INDEX = "association_type_index";
    final String ASSOCIATION_TYPE       = "association_type";
    final String ENTITY_TYPE_FIELD      = "entityType";

    // entity set field consts
    final String TYPE_FIELD     = "_type";
    final String ENTITY_SET     = "entitySet";
    final String ENTITY_SET_ID  = "entitySetId";
    final String SYNC_ID        = "syncId";
    final String PROPERTY_TYPES = "propertyTypes";
    final String ACLS           = "acls";
    final String NAME           = "name";
    final String NAMESPACE      = "namespace";
    final String TITLE          = "title";
    final String DESCRIPTION    = "description";
    final String ENTITY_TYPE_ID = "entityTypeId";
    final String ID             = "id";
    final String SRC            = "src";
    final String DST            = "dst";
    final String BIDIRECTIONAL  = "bidirectional";

    boolean saveEntitySetToElasticsearch( EntitySet entitySet, List<PropertyType> propertyTypes, Principal principal );

    boolean createSecurableObjectIndex( UUID entitySetId, UUID syncId, List<PropertyType> propertyTypes );

    boolean deleteEntitySet( UUID entitySetId );

    boolean deleteEntitySetForSyncId( UUID entitySetId, UUID syncId );

    SearchResult executeEntitySetMetadataSearch(
            Optional<String> optionalSearchTerm,
            Optional<UUID> optionalEntityType,
            Optional<Set<UUID>> optionalPropertyTypes,
            Set<AclKey> authorizedAclKeys,
            int start,
            int maxHits );

    boolean updateEntitySetMetadata( EntitySet entitySet );

    boolean updatePropertyTypesInEntitySet( UUID entitySetId, List<PropertyType> newPropertyTypes );

    boolean createOrganization( Organization organization, Principal principal );

    boolean deleteOrganization( UUID organizationId );

    SearchResult executeOrganizationSearch(
            String searchTerm,
            Set<AclKey> authorizedOrganizationIds,
            int start,
            int maxHits );

    boolean updateOrganization( UUID id, Optional<String> optionalTitle, Optional<String> optionalDescription );

    boolean createEntityData( UUID entitySetId, UUID syncId, String entityId, Map<UUID, Object> propertyValues );

    boolean deleteEntityData( UUID entitySetId, UUID syncId, String entityId );

    SearchResult executeEntitySetDataSearch(
            UUID entitySetId,
            UUID syncId,
            String searchTerm,
            int start,
            int maxHits,
            Set<UUID> authorizedPropertyTypes );

    List<EntityKey> executeEntitySetDataSearchAcrossIndices(
            Map<UUID, UUID> entitySetAndSyncIds,
            Map<UUID, DelegatedStringSet> fieldSearches,
            int size,
            boolean explain );

    SearchResult executeAdvancedEntitySetDataSearch(
            UUID entitySetId,
            UUID syncId,
            List<SearchDetails> searches,
            int start,
            int maxHits,
            Set<UUID> authorizedPropertyTypes );

    boolean saveEntityTypeToElasticsearch( EntityType entityType );

    boolean saveAssociationTypeToElasticsearch( AssociationType associationType );

    boolean savePropertyTypeToElasticsearch( PropertyType propertyType );

    boolean deleteEntityType( UUID entityTypeId );

    boolean deleteAssociationType( UUID associationTypeId );

    boolean deletePropertyType( UUID propertyTypeId );

    SearchResult executeEntityTypeSearch( String searchTerm, int start, int maxHits );

    SearchResult executeAssociationTypeSearch( String searchTerm, int start, int maxHits );

    SearchResult executePropertyTypeSearch( String searchTerm, int start, int maxHits );

    SearchResult executeFQNEntityTypeSearch( String namespace, String name, int start, int maxHits );

    SearchResult executeFQNPropertyTypeSearch( String namespace, String name, int start, int maxHits );

    boolean clearAllData();

    double getModelScore( double[][] features );

    boolean triggerPropertyTypeIndex( List<PropertyType> propertyTypes );

    boolean triggerEntityTypeIndex( List<EntityType> entityTypes );

    boolean triggerAssociationTypeIndex( List<AssociationType> associationTypes );

}
