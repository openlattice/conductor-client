package com.dataloom.data;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.data.requests.Association;
import com.dataloom.data.requests.Entity;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.core.objects.EdgeKey;
import com.google.common.collect.SetMultimap;

public interface DataGraphManager {
    
    Iterable<SetMultimap<FullQualifiedName, Object>> getEntitySetData(
            UUID entitySetId,
            UUID syncId,
            Map<UUID, PropertyType> authorizedPropertyTypes );
    
    Iterable<SetMultimap<FullQualifiedName, Object>> getLinkedEntitySetData(
            UUID linkedEntitySetId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets );
    
    void deleteEntitySetData( UUID entitySetId );
    
    void createEntityData(
            UUID entitySetId,
            UUID syncId,
            Map<String, SetMultimap<UUID, Object>> entities,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType );
    
    void updateAssociation(
            EdgeKey key,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType );
    
    void createAssociationData(
            UUID entitySetId,
            UUID syncId,
            Set<Association> associations,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType );
    
    void createEntityAndAssociationData(
            Iterable<Entity> entities,
            Iterable<Association> associations,
            Map<UUID, Map<UUID, EdmPrimitiveTypeKind>> authorizedPropertiesByEntitySetId );

}
