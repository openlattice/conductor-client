package com.dataloom.hazelcast.serializers;

import com.dataloom.apps.processors.UpdateAppMetadataProcessor;
import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class UpdateAppMetadataProcessorStreamSerializer implements
        SelfRegisteringStreamSerializer<UpdateAppMetadataProcessor> {
    @Override public Class<? extends UpdateAppMetadataProcessor> getClazz() {
        return UpdateAppMetadataProcessor.class;
    }

    @Override public void write(
            ObjectDataOutput out, UpdateAppMetadataProcessor object ) throws IOException {
        MetadataUpdate update = object.getUpdate();
        OptionalStreamSerializers.serialize( out, update.getTitle(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getDescription(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getName(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getUrl(), ObjectDataOutput::writeUTF );
    }

    @Override public UpdateAppMetadataProcessor read( ObjectDataInput in ) throws IOException {
        Optional<String> title = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<String> description = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<String> name = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<String> url = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );

        MetadataUpdate update = new MetadataUpdate(
                title,
                description,
                name,
                Optional.absent(),
                Optional.absent(),
                Optional.absent(),
                Optional.absent(),
                url );
        return new UpdateAppMetadataProcessor( update );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.UPDATE_APP_METADATA_PROCESSOR.ordinal();
    }

    @Override public void destroy() {

    }
}
