/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.kryptnostic.conductor.codecs.pods;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.data.EntityKey;
import com.dataloom.mappers.ObjectMappers;
import com.dataloom.edm.type.Analyzer;
import com.dataloom.requests.RequestStatus;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.enums.EnumNameCodec;
import com.datastax.driver.extras.codecs.joda.LocalDateCodec;
import com.datastax.driver.extras.codecs.joda.LocalTimeCodec;
import com.kryptnostic.conductor.codecs.EntityKeyTypeCodec;
import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;
import com.kryptnostic.conductor.codecs.FullQualifiedNameTypeCodec;
import com.kryptnostic.conductor.codecs.TimestampDateTimeTypeCodec;
import com.kryptnostic.conductor.codecs.TreeSetCodec;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.dataloom.auditing.AuditableEventKey.AuditableEventType;

@Configuration
public class TypeCodecsPod {
    @Bean
    public EnumNameCodec<EdmPrimitiveTypeKind> edmPrimitiveTypeKindCodec() {
        return new EnumNameCodec<>( EdmPrimitiveTypeKind.class );
    }

    @Bean
    public TypeCodec<List<UUID>> listUUIDCodec() {
        return TypeCodec.list( TypeCodec.uuid() );
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
    public EnumNameCodec<AuditableEventType> auditableEventTypeCodec() {
        return new EnumNameCodec<>( AuditableEventType.class );
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

    @Bean
    public EnumNameCodec<Analyzer> analyzerCodec() {
        return new EnumNameCodec<>( Analyzer.class );
    }

    @Bean
    public TypeCodec<EntityKey> entitykeyCodec() {
        return new EntityKeyTypeCodec();
    }

}
