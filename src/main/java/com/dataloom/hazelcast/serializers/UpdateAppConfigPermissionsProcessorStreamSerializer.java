package com.dataloom.hazelcast.serializers;

import com.openlattice.apps.processors.UpdateAppConfigPermissionsProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class UpdateAppConfigPermissionsProcessorStreamSerializer implements
        SelfRegisteringStreamSerializer<UpdateAppConfigPermissionsProcessor> {
    @Override public Class<? extends UpdateAppConfigPermissionsProcessor> getClazz() {
        return UpdateAppConfigPermissionsProcessor.class;
    }

    @Override public void write(
            ObjectDataOutput out, UpdateAppConfigPermissionsProcessor object ) throws IOException {
        DelegatedPermissionEnumSetStreamSerializer.serialize( out, object.getPermissions() );
    }

    @Override public UpdateAppConfigPermissionsProcessor read( ObjectDataInput in ) throws IOException {
        return new UpdateAppConfigPermissionsProcessor( DelegatedPermissionEnumSetStreamSerializer.deserialize( in ) );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.UPDATE_APP_CONFIG_PERMISSIONS_PROCESSOR.ordinal();
    }

    @Override public void destroy() {

    }
}
