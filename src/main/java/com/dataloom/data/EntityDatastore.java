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

import com.codahale.metrics.annotation.Timed;
import com.dataloom.data.analytics.IncrementableWeightId;
import com.dataloom.data.storage.EntityBytes;
import com.dataloom.edm.type.PropertyType;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public interface EntityDatastore {

    /**
     * Reads data from an entity set.
     */
    EntitySetData<FullQualifiedName> getEntitySetData(
            UUID entitySetId,
            UUID syncId,
            LinkedHashSet<String> orderedPropertyNames,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    /**
     * Reads a single row from an entity set.
     */
    SetMultimap<FullQualifiedName, Object> getEntity(
            UUID entitySetId,
            UUID syncId,
            String entityId,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    /**
     * Asynchronously load an entity with specified properties
     */
    ListenableFuture<SetMultimap<UUID, ByteBuffer>> asyncLoadEntity(
            UUID entitySetId,
            String entityId,
            UUID syncId,
            Set<UUID> properties );

    /**
     * Asynchronously load an entity with all properties
     */
    ListenableFuture<EntityBytes> asyncLoadEntity(
            UUID entitySetId,
            String entityId,
            UUID syncId );

    // TODO remove vertices too
    void deleteEntitySetData( UUID entitySetId );

    void deleteEntity( EntityKey entityKey );

    Stream<SetMultimap<Object, Object>> getEntities(
            Collection<UUID> ids, Map<UUID, PropertyType> authorizedPropertyTypes );

    SetMultimap<FullQualifiedName, Object> getEntity(
            UUID id, Map<UUID, PropertyType> authorizedPropertyTypes );

    ListenableFuture<SetMultimap<FullQualifiedName, Object>> getEntityAsync(
            UUID entitySetId,
            UUID syncId,
            String entityId,
            Map<UUID, PropertyType> authorizedPropertyTypes );

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
     */
    Stream<ListenableFuture> updateEntityAsync(
            EntityKey entityKey,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType );

    Stream<EntityKey> getEntityKeysForEntitySet( UUID entitySetId, UUID syncId );

    Stream<SetMultimap<Object, Object>> getEntities(
            IncrementableWeightId[] utilizers,
            Map<UUID, PropertyType> authorizedPropertyTypes );

}
