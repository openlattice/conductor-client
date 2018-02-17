package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import com.openlattice.edm.type.Analyzer;
import com.openlattice.edm.type.EnumType;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class EnumTypeStreamSerializer implements SelfRegisteringStreamSerializer<EnumType> {

    private static final EdmPrimitiveTypeKind[] edmTypes  = EdmPrimitiveTypeKind.values();
    private static final Analyzer[]             analyzers = Analyzer.values();

    @Override
    public void write( ObjectDataOutput out, EnumType object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        FullQualifiedNameStreamSerializer.serialize( out, object.getType() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
        SetStreamSerializers.serialize( out, object.getMembers(), ObjectDataOutput::writeUTF );
        SetStreamSerializers.serialize( out, object.getSchemas(), FullQualifiedNameStreamSerializer::serialize );
        out.writeInt( object.getDatatype().ordinal() );
        out.writeBoolean( object.isFlags() );
        out.writeBoolean( object.isPIIfield() );
        out.writeInt( object.getAnalyzer().ordinal() );
    }

    @Override
    public EnumType read( ObjectDataInput in ) throws IOException {
        Optional<UUID> id = Optional.of( UUIDStreamSerializer.deserialize( in ) );
        FullQualifiedName type = FullQualifiedNameStreamSerializer.deserialize( in );
        String title = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );
        LinkedHashSet<String> members = SetStreamSerializers.orderedDeserialize( in,
                ObjectDataInput::readUTF );
        Set<FullQualifiedName> schemas = SetStreamSerializers.deserialize( in,
                FullQualifiedNameStreamSerializer::deserialize );
        Optional<EdmPrimitiveTypeKind> datatype = Optional.of( edmTypes[ in.readInt() ] );
        boolean flags = in.readBoolean();
        Optional<Boolean> piiField = Optional.of( in.readBoolean() );
        Optional<Analyzer> analyzer = Optional.of( analyzers[ in.readInt() ] );
        return new EnumType( id, type, title, description, members, schemas, datatype, flags, piiField, analyzer );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENUM_TYPE.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<EnumType> getClazz() {
        return EnumType.class;
    }

}
