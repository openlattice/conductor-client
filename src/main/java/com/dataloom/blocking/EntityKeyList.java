package com.dataloom.blocking;

import com.dataloom.data.EntityKey;

public class EntityKeyList {
    private final EntityKey[] entityKeys;

    public EntityKeyList( EntityKey[] entityKeys ) {
        this.entityKeys = entityKeys;
    }

    public EntityKey[] getEntityKeys() {
        return entityKeys;
    }
}
