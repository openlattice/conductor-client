package com.dataloom.hazelcast.serializers;

import com.openlattice.directory.pojo.Auth0UserBasic;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Set;

@Component
public class Auth0UserBasicStreamSerializer implements SelfRegisteringStreamSerializer<Auth0UserBasic> {
    @Override public Class<? extends Auth0UserBasic> getClazz() {
        return Auth0UserBasic.class;
    }

    @Override public void write( ObjectDataOutput out, Auth0UserBasic object ) throws IOException {
        out.writeUTF( object.getUserId() );
        out.writeUTF( object.getEmail() );
        out.writeUTF( object.getNickname() );
        SetStreamSerializers.fastStringSetSerialize( out, object.getRoles() );
        SetStreamSerializers.fastStringSetSerialize( out, object.getOrganizations() );
    }

    @Override public Auth0UserBasic read( ObjectDataInput in ) throws IOException {
        String userId = in.readUTF();
        String email = in.readUTF();
        String nickname = in.readUTF();
        Set<String> roles = SetStreamSerializers.fastStringSetDeserialize( in );
        Set<String> organizations = SetStreamSerializers.fastStringSetDeserialize( in );

        return new Auth0UserBasic( userId, email, nickname, roles, organizations );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.AUTH0_USER_BASIC.ordinal();
    }

    @Override public void destroy() {

    }
}
