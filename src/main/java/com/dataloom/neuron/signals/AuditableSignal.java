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

package com.dataloom.neuron.signals;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.Principal;
import com.dataloom.client.serialization.SerializationConstants;
import com.dataloom.neuron.SignalType;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

public class AuditableSignal extends Signal {

    private static final Logger logger = LoggerFactory.getLogger( AuditableSignal.class );

    private UUID           auditId;
    private UUID           timeId;
    private Optional<UUID> entityId;
    private Optional<UUID> blockId;

    public AuditableSignal(
            @JsonProperty( SerializationConstants.TYPE_FIELD ) SignalType type,
            @JsonProperty( SerializationConstants.ACL_KEY ) List<UUID> aclKey,
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal principal,
            @JsonProperty( SerializationConstants.DETAILS_FIELD ) Optional<String> details,
            @JsonProperty( SerializationConstants.AUDIT_ID ) UUID auditId,
            @JsonProperty( SerializationConstants.TIME_ID ) UUID timeId,
            @JsonProperty( SerializationConstants.ENTITY_ID ) Optional<UUID> entityId,
            @JsonProperty( SerializationConstants.BLOCK_ID ) Optional<UUID> blockId ) {

        super( type, Optional.of( aclKey ), Optional.of( principal ), details );

        this.auditId = auditId;
        this.timeId = timeId;
        this.entityId = entityId;
        this.blockId = blockId;
    }

    @JsonProperty( SerializationConstants.TIME_ID )
    public UUID getTimeId() {
        return timeId;
    }

    @JsonProperty( SerializationConstants.AUDIT_ID )
    public UUID getAuditId() {
        return auditId;
    }

    @JsonProperty( SerializationConstants.ENTITY_ID )
    public Optional<UUID> getEntityId() {
        return entityId;
    }

    @JsonProperty( SerializationConstants.BLOCK_ID )
    public Optional<UUID> getBlockId() {
        return blockId;
    }
}
