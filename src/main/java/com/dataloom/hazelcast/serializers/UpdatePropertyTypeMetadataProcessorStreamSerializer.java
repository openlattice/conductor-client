package com.dataloom.hazelcast.serializers;

import java.io.IOException;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.edm.types.processors.UpdatePropertyTypeMetadataProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class UpdatePropertyTypeMetadataProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<UpdatePropertyTypeMetadataProcessor> {

    @Override
    public void write( ObjectDataOutput out, UpdatePropertyTypeMetadataProcessor object ) throws IOException {
        MetadataUpdate update = object.getUpdate();
        OptionalStreamSerializers.serialize( out, update.getTitle(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getDescription(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getType(), FullQualifiedNameStreamSerializer::serialize );
        OptionalStreamSerializers.serialize( out, update.getPii(), ObjectDataOutput::writeBoolean );
    }

    @Override
    public UpdatePropertyTypeMetadataProcessor read( ObjectDataInput in ) throws IOException {
        Optional<String> title = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<String> description = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<FullQualifiedName> type = OptionalStreamSerializers.deserialize( in,
                FullQualifiedNameStreamSerializer::deserialize );
        Optional<Boolean> pii = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readBoolean );

        MetadataUpdate update = new MetadataUpdate(
                title,
                description,
                Optional.absent(),
                Optional.absent(),
                type,
                pii,
                Optional.absent(),
                Optional.absent() );
        return new UpdatePropertyTypeMetadataProcessor( update );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.UPDATE_PROPERTY_TYPE_METADATA_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<UpdatePropertyTypeMetadataProcessor> getClazz() {
        return UpdatePropertyTypeMetadataProcessor.class;
    }
}