package com.dataloom.hazelcast.serializers;

import java.io.IOException;

import com.dataloom.edm.types.processors.RemoveSrcEntityTypesFromAssociationTypeProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class RemoveSrcEntityTypesFromAssociationTypeProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<RemoveSrcEntityTypesFromAssociationTypeProcessor> {

    @Override
    public void write( ObjectDataOutput out, RemoveSrcEntityTypesFromAssociationTypeProcessor object )
            throws IOException {
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getEntityTypeIds() );
    }

    @Override
    public RemoveSrcEntityTypesFromAssociationTypeProcessor read( ObjectDataInput in ) throws IOException {
        return new RemoveSrcEntityTypesFromAssociationTypeProcessor(
                SetStreamSerializers.fastUUIDSetDeserialize( in ) );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.REMOVE_SRC_ENTITY_TYPES_FROM_ASSOCIATION_TYPE_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends RemoveSrcEntityTypesFromAssociationTypeProcessor> getClazz() {
        return RemoveSrcEntityTypesFromAssociationTypeProcessor.class;
    }

}
