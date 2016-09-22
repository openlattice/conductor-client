package com.kryptnostic.conductor.rpc;

import com.google.common.base.Optional;
import com.google.common.collect.SetMultimap;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.UUID;

/**
 * Created by yao on 9/20/16.
 */
public class CreateEntityRequest {
    private final Optional<UUID> aclId;
    private final Optional<UUID> syncId;
    private final String entitySetName;
    private final FullQualifiedName entityType;
    private final SetMultimap<String, Object> entities;

    public CreateEntityRequest(
            Optional<UUID> aclId,
            Optional<UUID> syncId,
            String entitySetName,
            FullQualifiedName entityType, SetMultimap<String, Object> entities ) {
        this.aclId = aclId;
        this.syncId = syncId;
        this.entitySetName = entitySetName;
        this.entityType = entityType;
        this.entities = entities;
    }
}
