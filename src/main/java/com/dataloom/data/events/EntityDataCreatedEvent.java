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

package com.dataloom.data.events;

import java.util.Map;
import java.util.UUID;

import com.google.common.base.Optional;

public class EntityDataCreatedEvent {
    
    private UUID entitySetId;
    private Optional<UUID> syncId;
    private String entityId;
    private Map<UUID, Object> propertyValues;
    
    public EntityDataCreatedEvent( UUID entitySetId, Optional<UUID> syncId, String entityId, Map<UUID, Object> propertyValues ) {
        this.entitySetId = entitySetId;
        this.syncId = syncId;
        this.entityId = entityId;
        this.propertyValues = propertyValues;
    }
    
    public UUID getEntitySetId() {
        return entitySetId;
    }
    
    public Optional<UUID> getOptionalSyncId() {
        return syncId;
    }
    
    public String getEntityId() {
        return entityId;
    }
    
    public Map<UUID, Object> getPropertyValues() {
        return propertyValues;
    }

}
