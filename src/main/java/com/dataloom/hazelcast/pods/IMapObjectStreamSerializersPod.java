package com.dataloom.hazelcast.pods;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kryptnostic.conductor.rpc.serializers.AceKeyStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.AclRootPrincipalPairStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.AclRootRequestDetailsPairStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.DelegatedPermissionEnumSetStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.DelegatedStringSetStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.EntitySetStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.EntityTypeStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.FullQualifiedNameStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.PermissionsRequestDetailsStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.PrincipalRequestIdPairStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.PrincipalStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.PropertyTypeStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.UUIDStreamSerializer;

@Configuration
public class IMapObjectStreamSerializersPod {

    @Bean
    UUIDStreamSerializer uuidss() {
        return new UUIDStreamSerializer();
    }

    @Bean
    public EntitySetStreamSerializer esss() {
        return new EntitySetStreamSerializer();
    }

    @Bean
    public EntityTypeStreamSerializer etss() {
        return new EntityTypeStreamSerializer();
    }

    @Bean
    public PropertyTypeStreamSerializer ptss() {
        return new PropertyTypeStreamSerializer();
    }

    @Bean
    public FullQualifiedNameStreamSerializer fqnss() {
        return new FullQualifiedNameStreamSerializer();
    }

    @Bean
    public AceKeyStreamSerializer akss() {
        return new AceKeyStreamSerializer();
    }

    @Bean
    public DelegatedStringSetStreamSerializer dssss() {
        return new DelegatedStringSetStreamSerializer();
    }

    @Bean
    public DelegatedPermissionEnumSetStreamSerializer dpesss() {
        return new DelegatedPermissionEnumSetStreamSerializer();
    }

    @Bean
    public PrincipalStreamSerializer pss() {
        return new PrincipalStreamSerializer();
    }

    @Bean
    public AclRootPrincipalPairStreamSerializer arppss() {
        return new AclRootPrincipalPairStreamSerializer();
    }

    @Bean
    public PermissionsRequestDetailsStreamSerializer prdss() {
        return new PermissionsRequestDetailsStreamSerializer();
    }

    @Bean
    public PrincipalRequestIdPairStreamSerializer pripss() {
        return new PrincipalRequestIdPairStreamSerializer();
    }

    @Bean
    public AclRootRequestDetailsPairStreamSerializer arrdpss() {
        return new AclRootRequestDetailsPairStreamSerializer();
    }

}
