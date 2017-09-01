package com.dataloom.hazelcast.serializers;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.edm.types.processors.UpdateEntitySetPropertyMetadataProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class UpdateEntitySetPropertyMetadataProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<UpdateEntitySetPropertyMetadataProcessor> {

    @Override
    public void write( ObjectDataOutput out, UpdateEntitySetPropertyMetadataProcessor object ) throws IOException {
        MetadataUpdate update = object.getUpdate();
        OptionalStreamSerializers.serialize( out, update.getTitle(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getDescription(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getDefaultShow(), ObjectDataOutput::writeBoolean );
    }

    @Override
    public UpdateEntitySetPropertyMetadataProcessor read( ObjectDataInput in ) throws IOException {
        Optional<String> title = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<String> description = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<Boolean> defaultShow = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readBoolean );

        MetadataUpdate update = new MetadataUpdate(
                title,
                description,
                Optional.absent(),
                Optional.absent(),
                Optional.absent(),
                Optional.absent(),
                defaultShow );
        return new UpdateEntitySetPropertyMetadataProcessor( update );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.UPDATE_ENTITY_SET_PROPERTY_METADATA_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends UpdateEntitySetPropertyMetadataProcessor> getClazz() {
        return UpdateEntitySetPropertyMetadataProcessor.class;
    }

}
