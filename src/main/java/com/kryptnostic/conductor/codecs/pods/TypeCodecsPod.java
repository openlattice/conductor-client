package com.kryptnostic.conductor.codecs.pods;

import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.requests.RequestStatus;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.enums.EnumNameCodec;
import com.datastax.driver.extras.codecs.joda.LocalDateCodec;
import com.datastax.driver.extras.codecs.joda.LocalTimeCodec;
import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;
import com.kryptnostic.conductor.codecs.FullQualifiedNameTypeCodec;
import com.kryptnostic.conductor.codecs.TimestampDateTimeTypeCodec;

@Configuration
public class TypeCodecsPod {
    @Bean
    public EnumNameCodec<EdmPrimitiveTypeKind> edmPrimitiveTypeKindCodec() {
        return new EnumNameCodec<>( EdmPrimitiveTypeKind.class );
    }

    @Bean
    public TypeCodec<Set<String>> setStringCodec() {
        return TypeCodec.set( TypeCodec.varchar() );
    }

    @Bean
    public TypeCodec<Set<UUID>> setUuidCodec() {
        return TypeCodec.set( TypeCodec.uuid() );
    }

    @Bean
    public TypeCodec<FullQualifiedName> fqnCodec() {
        return new FullQualifiedNameTypeCodec();
    }

    public TimestampDateTimeTypeCodec timestampDateTimeTypeCodec() {
        return TimestampDateTimeTypeCodec.getInstance();
    }

    @Bean
    public LocalDateCodec jodaLocalDateCodec() {
        return LocalDateCodec.instance;
    }

    @Bean
    public LocalTimeCodec jodaLocalTimeCodec() {
        return LocalTimeCodec.instance;
    }

    @Bean
    public EnumNameCodec<Permission> permissionCodec() {
        return new EnumNameCodec<>( Permission.class );
    }

    @Bean
    public EnumNameCodec<PrincipalType> principalTypeCodec() {
        return new EnumNameCodec<>( PrincipalType.class );
    }

    @Bean
    public EnumNameCodec<SecurableObjectType> securableObjectTypeCodec() {
        return new EnumNameCodec<>( SecurableObjectType.class );
    }

    @Bean
    public TypeCodec<EnumSet<Permission>> enumSetPermissionCodec() {
        return new EnumSetTypeCodec<Permission>( permissionCodec() );
    }

    @Bean
    public EnumNameCodec<RequestStatus> requestStatusCodec() {
        return new EnumNameCodec<>( RequestStatus.class );
    }

}
