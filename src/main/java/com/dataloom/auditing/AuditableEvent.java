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
import com.dataloom.authorization.securable.SecurableObjectType;
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.joda.time.DateTime;

import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class AuditableEvent {
    private final AuditableEventKey   eventKey;
    private final SecurableObjectType objectType;
    private final String                   eventDetails;

    public AuditableEvent(
            List<UUID> aclKey,
            Principal principal,
            SecurableObjectType objectType,
            EnumSet<Permission> eventType,
            String eventDetails ) {
        this( new AuditableEventKey( aclKey, principal, eventType ), objectType, eventType, eventDetails );
    }

    public AuditableEvent(
            List<UUID> aclKey,
            UUID timestamp,
            Principal principal,
            SecurableObjectType objectType,
            EnumSet<Permission> eventType,
            String eventDetails ) {
        this( new AuditableEventKey( aclKey, timestamp, principal, eventType ), objectType, eventType, eventDetails );

    }

    public AuditableEvent(
            AuditableEventKey eventKey,
            SecurableObjectType objectType,
            EnumSet<Permission> requestedPermission,
            String eventDetails ) {
        this.eventKey = checkNotNull( eventKey );
        this.objectType = checkNotNull( objectType );
        this.eventDetails = checkNotNull( eventDetails );
    }

    @JsonIgnore
    public AuditableEventKey getEventKey() {
        return eventKey;
    }

    public SecurableObjectType getObjectType() {
        return objectType;
    }

    public List<UUID> getAclKey() {
        return eventKey.getAclKey();
    }

    @JsonIgnore
    public UUID getUuidTimestamp() {
        return eventKey.getTimestamp();
    }

    public DateTime getTimestamp() {
        return new DateTime(UUIDs.unixTimestamp(eventKey.getTimestamp()));
    }

    public Principal getPrincipal() {
        return eventKey.getPrincipal();
    }

    public EnumSet<Permission> getEventType() {
        return eventKey.getEventType();
    }

    public String getEventDetails() {
        return eventDetails;
    }
}
