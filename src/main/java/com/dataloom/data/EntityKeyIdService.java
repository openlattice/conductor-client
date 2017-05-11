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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public interface EntityKeyIdService {

    /**
     * Retrieves the assigned id for an entity key. Assigns one if entity key hasn't been assigned.
     * @param entityKey The entity key for which to retrieve an assigned id.
     * @return The id assigned to entity key.
     */
    UUID getEntityKeyId( EntityKey entityKey );

    Map<EntityKey, UUID> getEntityKeyIds( Set<EntityKey> entityKeys );

    ListenableFuture<UUID> getEntityKeyIdAsync( EntityKey entityKey );

    /**
     * Retrieves the entity key id previously assigned for this entity key
     *
     * @param entityKeyId The id of the entity key to be retrieved.
     * @return An entity key id, if this entity key previously had one assigned.
     */
    //TODO: Change this to throwing an exception.
    Optional<EntityKey> tryGetEntityKey( UUID entityKeyId );

    EntityKey getEntityKey( UUID entityKeyId );

    ListenableFuture<EntityKey> getEntityKeyAsync( UUID entityKeyId );
}
