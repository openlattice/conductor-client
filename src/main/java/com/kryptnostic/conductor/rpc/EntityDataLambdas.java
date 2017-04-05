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

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class EntityDataLambdas implements Function<ConductorElasticsearchApi, Boolean>, Serializable {
    private static final long serialVersionUID = -1071651645473672891L;
    
    private UUID entitySetId;
    private UUID syncId;
    private String entityId;
    private Map<UUID, Object> propertyValues;

    public EntityDataLambdas( UUID entitySetId, UUID syncId, String entityId, Map<UUID, Object> propertyValues ) {
        this.entitySetId = entitySetId;
        this.syncId = syncId;
        this.entityId = entityId;
        this.propertyValues = propertyValues;
    }

    @Override
    public Boolean apply( ConductorElasticsearchApi api ) {
        return api.createEntityData( entitySetId, syncId, entityId, propertyValues );
    }

}
