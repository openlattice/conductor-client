package com.dataloom.hazelcast.serializers;

import java.io.IOException;

import com.dataloom.edm.types.processors.AddSrcEntityTypesToAssociationTypeProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class AddSrcEntityTypesToAssociationTypeProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<AddSrcEntityTypesToAssociationTypeProcessor> {

    @Override
    public void write( ObjectDataOutput out, AddSrcEntityTypesToAssociationTypeProcessor object ) throws IOException {
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getEntityTypeIds() );
    }

    @Override
    public AddSrcEntityTypesToAssociationTypeProcessor read( ObjectDataInput in ) throws IOException {
        return new AddSrcEntityTypesToAssociationTypeProcessor( SetStreamSerializers.fastUUIDSetDeserialize( in ) );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ADD_SRC_ENTITY_TYPES_TO_ASSOCIATION_TYPE_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends AddSrcEntityTypesToAssociationTypeProcessor> getClazz() {
        return AddSrcEntityTypesToAssociationTypeProcessor.class;
    }

}
