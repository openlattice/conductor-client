package com.dataloom.hazelcast.serializers;

import com.openlattice.apps.AppType;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

@Component
public class AppTypeStreamSerializer implements SelfRegisteringStreamSerializer<AppType> {
    @Override public Class<? extends AppType> getClazz() {
        return AppType.class;
    }

    @Override public void write( ObjectDataOutput out, AppType object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        FullQualifiedNameStreamSerializer.serialize( out, object.getType() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
        UUIDStreamSerializer.serialize( out, object.getEntityTypeId() );
    }

    @Override public AppType read( ObjectDataInput in ) throws IOException {
        UUID id = UUIDStreamSerializer.deserialize( in );
        FullQualifiedName type = FullQualifiedNameStreamSerializer.deserialize( in );
        String title = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );
        UUID entityTypeId = UUIDStreamSerializer.deserialize( in );
        return new AppType( id, type, title, description, entityTypeId );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.APP_TYPE.ordinal();
    }

    @Override public void destroy() {

    }
}
