package com.dataloom.hazelcast.serializers;

import java.io.IOException;

import com.dataloom.edm.types.processors.AddDstEntityTypesToAssociationTypeProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class AddDstEntityTypesToAssociationTypeProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<AddDstEntityTypesToAssociationTypeProcessor> {

    @Override
    public void write( ObjectDataOutput out, AddDstEntityTypesToAssociationTypeProcessor object ) throws IOException {
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getEntityTypeIds() );
    }

    @Override
    public AddDstEntityTypesToAssociationTypeProcessor read( ObjectDataInput in ) throws IOException {
        return new AddDstEntityTypesToAssociationTypeProcessor( SetStreamSerializers.fastUUIDSetDeserialize( in ) );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ADD_DST_ENTITY_TYPES_TO_ASSOCIATION_TYPE_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends AddDstEntityTypesToAssociationTypeProcessor> getClazz() {
        return AddDstEntityTypesToAssociationTypeProcessor.class;
    }

}
