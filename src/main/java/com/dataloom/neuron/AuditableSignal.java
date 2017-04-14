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

package com.dataloom.neuron;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.Principal;

public class AuditableSignal extends Signal {

    private static final Logger logger = LoggerFactory.getLogger( AuditableSignal.class );

    private List<UUID> aclKey;
    private Principal  principal;
    private UUID       timeId;
    private UUID       entityId;
    private UUID       auditId;
    private UUID       blockId;

    public AuditableSignal(
            SignalType type,
            List<UUID> aclKey,
            Principal principal,
            UUID timeId,
            UUID entityId,
            UUID auditId,
            UUID blockId ) {

        super( type );

        this.aclKey = aclKey;
        this.principal = principal;
        this.timeId = timeId;
        this.entityId = entityId;
        this.auditId = auditId;
        this.blockId = blockId;
    }

    public List<UUID> getAclKey() {
        return aclKey;
    }

    public Principal getPrincipal() {
        return principal;
    }

    public UUID getTimeId() {
        return timeId;
    }

    public UUID getEntityId() {
        return entityId;
    }

    public UUID getAuditId() {
        return auditId;
    }

    public UUID getBlockId() {
        return blockId;
    }
}
