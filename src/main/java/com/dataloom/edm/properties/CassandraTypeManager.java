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

package com.dataloom.edm.properties;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.Iterables;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;

public class CassandraTypeManager {
    private final Session           session;
    private final PreparedStatement entityTypesContainPropertyType;
    private final Select            getEntityTypeIds;
    private final Select            getEntityTypes;
    private final Select            getPropertyTypeIds;
    private final Select            getPropertyTypes;
    private final PreparedStatement getPropertyTypesInNamespace;

    public CassandraTypeManager( String keyspace, Session session ) {
        this.session = session;
        this.entityTypesContainPropertyType = session.prepare(
                QueryBuilder.select().all()
                        .from( keyspace, Tables.ENTITY_TYPES.getName() ).allowFiltering()
                        .where( QueryBuilder
                                .contains( CommonColumns.PROPERTIES.cql(), CommonColumns.PROPERTIES.bindMarker() ) ) );
        this.getEntityTypeIds = QueryBuilder.select( CommonColumns.ID.cql() ).distinct().from( keyspace,
                Tables.ENTITY_TYPES.getName() );
        this.getEntityTypes = QueryBuilder.select().all().from( keyspace,
                Tables.ENTITY_TYPES.getName() );
        this.getPropertyTypeIds = QueryBuilder.select( CommonColumns.ID.cql() ).distinct().from( keyspace,
                Tables.PROPERTY_TYPES.getName() );
        this.getPropertyTypes = QueryBuilder.select().all()
                .from( keyspace, Tables.PROPERTY_TYPES.getName() );
        this.getPropertyTypesInNamespace = session.prepare(
                QueryBuilder.select().all()
                        .from( keyspace, Tables.PROPERTY_TYPES.getName() )
                        .where( QueryBuilder
                                .eq( CommonColumns.NAMESPACE.cql(), CommonColumns.NAMESPACE.bindMarker() ) ) );
    }

    public Iterable<PropertyType> getPropertyTypesInNamespace( String namespace ) {
        return Iterables.transform(
                session.execute(
                        getPropertyTypesInNamespace.bind().setString( CommonColumns.NAMESPACE.cql(), namespace ) ),
                RowAdapters::propertyType );
    }

    public Iterable<PropertyType> getPropertyTypes() {
        return Iterables.transform( session.execute( getPropertyTypes ), RowAdapters::propertyType );
    }

    public Iterable<UUID> getPropertyTypeIds() {
        return Iterables.transform( session.execute( getPropertyTypeIds ),
                row -> row.getUUID( CommonColumns.ID.cql() ) );
    }

    public Iterable<UUID> getEntityTypeIds() {
        return Iterables.transform( session.execute( getEntityTypeIds ),
                row -> row.getUUID( CommonColumns.ID.cql() ) );
    }

    public Iterable<EntityType> getEntityTypes() {
        return Iterables.transform( session.execute( getEntityTypes ), RowAdapters::entityType );
    }

    public Set<EntityType> getEntityTypesContainingPropertyTypes( Set<UUID> properties ) {
        return getEntityTypesContainingPropertyTypesAsStream( properties )
                .collect( Collectors.toSet() );
    }

    public Stream<EntityType> getEntityTypesContainingPropertyTypesAsStream( Set<UUID> properties ) {
        Stream<ResultSetFuture> rsfs = properties.stream().map( this::getEntityTypesContainingPropertyType );
        return StreamUtil.getRowsAndFlatten( rsfs ).map( RowAdapters::entityType );
    }

    private ResultSetFuture getEntityTypesContainingPropertyType( UUID propertyId ) {
        return session.executeAsync(
                entityTypesContainPropertyType.bind().setUUID( CommonColumns.PROPERTIES.cql(), propertyId ) );
    }

}
