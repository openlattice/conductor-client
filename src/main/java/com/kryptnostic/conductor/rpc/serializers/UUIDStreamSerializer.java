package com.kryptnostic.conductor.rpc.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractUUIDStreamSerializer;

public class UUIDStreamSerializer extends AbstractUUIDStreamSerializer {

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.UUID.ordinal();
    }

}
