package com.dataloom.hazelcast.serializers;

import com.dataloom.apps.App;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.UUID;

@Component
public class AppStreamSerializer implements SelfRegisteringStreamSerializer<App> {
    @Override public Class<? extends App> getClazz() {
        return App.class;
    }

    @Override public void write( ObjectDataOutput out, App object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        out.writeUTF( object.getName() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getUrl() );
        out.writeUTF( object.getDescription() );
        out.writeInt( object.getAppTypeIds().size() );
        for ( UUID id : object.getAppTypeIds() ) {
            UUIDStreamSerializer.serialize( out, id );
        }
    }

    @Override public App read( ObjectDataInput in ) throws IOException {
        UUID id = UUIDStreamSerializer.deserialize( in );
        String name = in.readUTF();
        String title = in.readUTF();
        String url = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );

        int numConfigTypeIds = in.readInt();
        LinkedHashSet<UUID> configTypeIds = new LinkedHashSet<>();
        for ( int i = 0; i < numConfigTypeIds; i++ ) {
            configTypeIds.add( UUIDStreamSerializer.deserialize( in ) );
        }
        return new App( id, name, title, description, configTypeIds, url );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.APP.ordinal();
    }

    @Override public void destroy() {

    }
}
