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

package com.dataloom.data;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import com.datastax.driver.core.ResultSet;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.type.PropertyType;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.SetMultimap;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public interface EntityDatastore {

    /**
     * Reads data from an entity set.
     *
     * @param entitySetId
     * @param syncId
     * @param authorizedPropertyTypes
     * @return
     */
    EntitySetData getEntitySetData(
            UUID entitySetId,
            UUID syncId,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    /**
     * Reads a single row from an entity set.
     * 
     * @param entitySetId
     * @param syncId
     * @param entityId
     * @param authorizedPropertyTypes
     * @return
     */
    SetMultimap<FullQualifiedName, Object> getEntity(
            UUID entitySetId,
            UUID syncId,
            String entityId,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    /**
     * Reads data from an linked entity set.
     *
     * @param linkedEntitySetId
     * @param authorizedPropertyTypesForEntitySets
     * @return
     */
    EntitySetData getLinkedEntitySetData(
            UUID linkedEntitySetId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets );

    // TODO remove vertices too
    void deleteEntitySetData( UUID entitySetId );

    void deleteEntity( EntityKey entityKey );

    /**
     * @param entityKey
     * @param entityDetails
     * @param authorizedPropertiesWithDataType
     */
    void updateEntity(
            EntityKey entityKey,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType );

    /**
     * Performs async storage of an entity.
     * 
     * @param entityKey
     * @param entityDetails
     * @param authorizedPropertiesWithDataType
     * @return
     */
    ListenableFuture<List<ResultSet>> updateEntityAsync(
            EntityKey entityKey,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType );

    Stream<EntityKey> getEntityKeysForEntitySet( UUID entitySetId, UUID syncId );
    
    void writeVertexCount( ByteBuffer queryId, UUID vertexId, double score );
    
    Iterable<UUID> readTopUtilizers( ByteBuffer queryId, int numResults );

}
