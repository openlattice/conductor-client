package com.dataloom.hazelcast.serializers;

import java.io.IOException;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.edm.types.processors.UpdateEntityTypeMetadataProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class UpdateEntityTypeMetadataProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<UpdateEntityTypeMetadataProcessor> {

    @Override
    public void write( ObjectDataOutput out, UpdateEntityTypeMetadataProcessor object ) throws IOException {
        MetadataUpdate update = object.getUpdate();
        OptionalStreamSerializers.serialize( out, update.getTitle(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getDescription(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getType(), FullQualifiedNameStreamSerializer::serialize );
    }

    @Override
    public UpdateEntityTypeMetadataProcessor read( ObjectDataInput in ) throws IOException {
        Optional<String> title = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<String> description = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<FullQualifiedName> type = OptionalStreamSerializers.deserialize( in,
                FullQualifiedNameStreamSerializer::deserialize );

        MetadataUpdate update = new MetadataUpdate(
                title,
                description,
                Optional.absent(),
                Optional.absent(),
                type,
                Optional.absent() );
        return new UpdateEntityTypeMetadataProcessor( update );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.UPDATE_ENTITY_TYPE_METADATA_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<UpdateEntityTypeMetadataProcessor> getClazz() {
        return UpdateEntityTypeMetadataProcessor.class;
    }
}