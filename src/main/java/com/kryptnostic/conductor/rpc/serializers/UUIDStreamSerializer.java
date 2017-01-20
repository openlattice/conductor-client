package com.kryptnostic.conductor.rpc.serializers;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractUUIDStreamSerializer;

@Component
public class UUIDStreamSerializer extends AbstractUUIDStreamSerializer {

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.UUID.ordinal();
    }

}
