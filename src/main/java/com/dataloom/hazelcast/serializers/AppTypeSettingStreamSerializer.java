package com.dataloom.hazelcast.serializers;

import com.openlattice.apps.AppTypeSetting;
import com.openlattice.authorization.Permission;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.EnumSet;
import java.util.UUID;

@Component
public class AppTypeSettingStreamSerializer implements SelfRegisteringStreamSerializer<AppTypeSetting> {
    @Override public Class<? extends AppTypeSetting> getClazz() {
        return AppTypeSetting.class;
    }

    @Override public void write( ObjectDataOutput out, AppTypeSetting object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getEntitySetId() );
        DelegatedPermissionEnumSetStreamSerializer.serialize( out, object.getPermissions() );
    }

    @Override public AppTypeSetting read( ObjectDataInput in ) throws IOException {
        UUID entitySetId = UUIDStreamSerializer.deserialize( in );
        EnumSet<Permission> permissions = DelegatedPermissionEnumSetStreamSerializer.deserialize( in );
        return new AppTypeSetting( entitySetId, permissions );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.APP_TYPE_SETTING.ordinal();
    }

    @Override public void destroy() {

    }
}
