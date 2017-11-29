package com.dataloom.hazelcast.serializers;

import com.dataloom.apps.processors.UpdateAppTypeMetadataProcessor;
import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class UpdateAppTypeMetadataProcessorStreamSerializer implements SelfRegisteringStreamSerializer<UpdateAppTypeMetadataProcessor> {
    @Override public Class<? extends UpdateAppTypeMetadataProcessor> getClazz() {
        return UpdateAppTypeMetadataProcessor.class;
    }

    @Override public void write(
            ObjectDataOutput out, UpdateAppTypeMetadataProcessor object ) throws IOException {
        MetadataUpdate update = object.getUpdate();
        OptionalStreamSerializers.serialize( out, update.getTitle(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getDescription(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getType(), FullQualifiedNameStreamSerializer::serialize );
    }

    @Override public UpdateAppTypeMetadataProcessor read( ObjectDataInput in ) throws IOException {
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
                Optional.absent(),
                Optional.absent() );
        return new UpdateAppTypeMetadataProcessor( update );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.UPDATE_APP_TYPE_METADATA_PROCESSOR.ordinal();
    }

    @Override public void destroy() {

    }
}
