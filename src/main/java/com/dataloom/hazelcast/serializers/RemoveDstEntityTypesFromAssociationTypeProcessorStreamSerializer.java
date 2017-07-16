package com.dataloom.hazelcast.serializers;

import java.io.IOException;

import com.dataloom.edm.types.processors.RemoveDstEntityTypesFromAssociationTypeProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class RemoveDstEntityTypesFromAssociationTypeProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<RemoveDstEntityTypesFromAssociationTypeProcessor> {

    @Override
    public void write( ObjectDataOutput out, RemoveDstEntityTypesFromAssociationTypeProcessor object )
            throws IOException {
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getEntityTypeIds() );
    }

    @Override
    public RemoveDstEntityTypesFromAssociationTypeProcessor read( ObjectDataInput in ) throws IOException {
        return new RemoveDstEntityTypesFromAssociationTypeProcessor(
                SetStreamSerializers.fastUUIDSetDeserialize( in ) );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.REMOVE_DST_ENTITY_TYPES_FROM_ASSOCIATION_TYPE_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub

    }

    @Override
    public Class<? extends RemoveDstEntityTypesFromAssociationTypeProcessor> getClazz() {
        return RemoveDstEntityTypesFromAssociationTypeProcessor.class;
    }

}
