package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.openlattice.edm.requests.MetadataUpdate;
import com.dataloom.edm.types.processors.UpdateEntitySetMetadataProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class UpdateEntitySetMetadataProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<UpdateEntitySetMetadataProcessor> {

    @Override
    public void write( ObjectDataOutput out, UpdateEntitySetMetadataProcessor object ) throws IOException {
        MetadataUpdate update = object.getUpdate();
        OptionalStreamSerializers.serialize( out, update.getTitle(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getDescription(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getName(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serializeSet( out, update.getContacts(), ObjectDataOutput::writeUTF );
    }

    @Override
    public UpdateEntitySetMetadataProcessor read( ObjectDataInput in ) throws IOException {
        Optional<String> title = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<String> description = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<String> name = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<Set<String>> contacts = OptionalStreamSerializers.deserializeSet( in, ObjectDataInput::readUTF );

        MetadataUpdate update = new MetadataUpdate(
                title,
                description,
                name,
                contacts,
                Optional.absent(),
                Optional.absent(),
                Optional.absent(),
                Optional.absent() );
        return new UpdateEntitySetMetadataProcessor( update );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.UPDATE_ENTITY_SET_METADATA_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<UpdateEntitySetMetadataProcessor> getClazz() {
        return UpdateEntitySetMetadataProcessor.class;
    }
}