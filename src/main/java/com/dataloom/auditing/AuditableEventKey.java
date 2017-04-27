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

package com.dataloom.auditing;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.datastax.driver.core.utils.UUIDs;

import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */

@Deprecated
public class AuditableEventKey {
    private final List<UUID>          aclKey;
    private final UUID                timestamp;
    private final Principal           principal;
    private final EnumSet<Permission> eventType;

    public AuditableEventKey(
            List<UUID> aclKey,
            Principal principal,
            EnumSet<Permission> eventType ) {
        this( aclKey, UUIDs.timeBased(), principal, eventType );
    }

    public AuditableEventKey(
            List<UUID> aclKey,
            UUID timestamp,
            Principal principal,
            EnumSet<Permission> eventType ) {
        this.aclKey = checkNotNull( aclKey );
        this.timestamp = checkNotNull( timestamp );
        this.principal = checkNotNull( principal );
        this.eventType = checkNotNull( eventType );
    }

    public List<UUID> getAclKey() {
        return aclKey;
    }

    public UUID getTimestamp() {
        return timestamp;
    }

    public Principal getPrincipal() {
        return principal;
    }

    public EnumSet<Permission> getEventType() {
        return eventType;
    }

    public static enum AuditableEventType {
        READ,
        WRITE,
        DELETE,
        JOIN,
        QUERY
    }
}
