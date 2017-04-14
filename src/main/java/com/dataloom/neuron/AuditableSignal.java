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
