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

import com.datastax.driver.core.ResultSetFuture;

import java.util.Optional;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public interface EntityKeyIdService {
    /**
     * Retrieves the entity key id previously assigned for this entity key
     *
     * @param entityKey
     * @return An entity key id, if this entity key previously had one assigned.
     */
    //TODO: Change this to throwing an exception.

    Optional<EntityKey> tryGetEntityKey( UUID entityKeyId );

    EntityKey getEntityKey( UUID entityKeyId );

    ResultSetFuture getEntityKeyAsync( UUID entityKeyId );

    ResultSetFuture setEntityKeyId( EntityKey entityKey, UUID entityKeyId )
}
