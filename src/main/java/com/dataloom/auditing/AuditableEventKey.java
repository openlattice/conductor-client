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
