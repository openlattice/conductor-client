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

import static com.google.common.base.Preconditions.checkNotNull;

public class AuditableSignal extends Signal {

    private static final Logger logger = LoggerFactory.getLogger( AuditableSignal.class );

    private UUID auditId;
    private UUID timeId;
    private UUID dataId;
    private UUID blockId;

    public AuditableSignal(
            @JsonProperty( SerializationConstants.TYPE_FIELD ) SignalType type,
            @JsonProperty( SerializationConstants.ACL_KEY ) List<UUID> aclKey,
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal principal,
            @JsonProperty( SerializationConstants.DETAILS_FIELD ) String details,
            @JsonProperty( SerializationConstants.AUDIT_ID ) UUID auditId,
            @JsonProperty( SerializationConstants.TIME_ID ) UUID timeId,
            @JsonProperty( SerializationConstants.DATA_ID ) UUID dataId,
            @JsonProperty( SerializationConstants.BLOCK_ID ) UUID blockId ) {

        super( type, checkNotNull( aclKey ), checkNotNull( principal ), details );

        this.auditId = checkNotNull( auditId );
        this.timeId = checkNotNull( timeId );
        this.dataId = dataId;
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

    @JsonProperty( SerializationConstants.DATA_ID )
    public UUID getDataId() {
        return dataId;
    }

    @JsonProperty( SerializationConstants.BLOCK_ID )
    public UUID getBlockId() {
        return blockId;
    }
}
