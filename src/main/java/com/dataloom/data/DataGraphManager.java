package com.dataloom.data;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

import com.dataloom.analysis.requests.TopUtilizerDetails;
import com.dataloom.data.requests.Association;
import com.dataloom.data.requests.Entity;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.edge.EdgeKey;
import com.google.common.collect.SetMultimap;

public interface DataGraphManager {
    /*
     * Entity set methods
     */
    EntitySetData getEntitySetData(
            UUID entitySetId,
            UUID syncId,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    EntitySetData getLinkedEntitySetData(
            UUID linkedEntitySetId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets );

    // TODO remove vertices too
    void deleteEntitySetData( UUID entitySetId );

    /*
     * CRUD methods for entity
     */
    void updateEntity(
            UUID elementId,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType );

    void updateEntity(
            EntityKey elementReference,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType );

    void deleteEntity( UUID elementId );

    void deleteAssociation( EdgeKey key );

    /*
     * Bulk endpoints for entities/associations
     */

    UUID createEntity(
            UUID entitySetId,
            UUID syncId,
            String entityId,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType )
            throws ExecutionException, InterruptedException;

    void createEntities(
            UUID entitySetId,
            UUID syncId,
            Map<String, SetMultimap<UUID, Object>> entities,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType )
            throws ExecutionException, InterruptedException;
    
    void createAssociations(
            UUID entitySetId,
            UUID syncId,
            Set<Association> associations,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType )
            throws ExecutionException, InterruptedException;

    void createEntitiesAndAssociations(
            Set<Entity> entities,
            Set<Association> associations,
            Map<UUID, Map<UUID, EdmPrimitiveTypeKind>> authorizedPropertiesByEntitySetId )
            throws ExecutionException, InterruptedException;

    public Iterable<SetMultimap<Object, Object>> getTopUtilizers(
            UUID entitySetId,
            UUID syncId,
            List<TopUtilizerDetails> topUtilizerDetails,
            int numResults,
            Map<UUID, PropertyType> authorizedPropertyTypes )
            throws InterruptedException, ExecutionException;

}
