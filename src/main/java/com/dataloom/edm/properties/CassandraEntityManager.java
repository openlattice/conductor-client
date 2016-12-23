package com.dataloom.edm.properties;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.spark_project.guava.collect.ImmutableSet;
import org.spark_project.guava.collect.Iterables;

import com.dataloom.edm.internal.EntityType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;

public class CassandraEntityManager {
    private final Session           session;
    private final PreparedStatement entityTypesContainPropertyType;
    private final Select            getEntityTypes;

    public CassandraEntityManager( String keyspace, Session session ) {
        this.session = session;
        this.entityTypesContainPropertyType = session.prepare( QueryBuilder.select().all()
                .from( keyspace, Tables.PROPERTY_TYPES.getName() ).where( QueryBuilder
                        .contains( CommonColumns.PROPERTIES.cql(), CommonColumns.PROPERTIES.bindMarker() ) ) );
        this.getEntityTypes = QueryBuilder.select( CommonColumns.ID.cql() ).distinct().from( keyspace,
                Tables.ENTITY_TYPES.getName() );
    }

    public Set<UUID> getAllEntityTypeIds() {
        return ImmutableSet.copyOf(
                Iterables.transform( session.execute( getEntityTypes ),
                        row -> row.getUUID( CommonColumns.ID.cql() ) ) );
    }

    public Set<EntityType> getEntityTypesContainingPropertyTypes( Set<FullQualifiedName> properties ) {
        return properties.stream().map( this::getEntityTypesContainingPropertyType )
                .map( ResultSetFuture::getUninterruptibly )
                .map( rs -> Iterables.transform( rs, RowAdapters::entityType ).spliterator() )
                .flatMap( si -> StreamSupport.stream( si, false ) )
                .collect( Collectors.toSet() );
    }

    private ResultSetFuture getEntityTypesContainingPropertyType( FullQualifiedName property ) {
        return session.executeAsync(
                entityTypesContainPropertyType.bind().set( CommonColumns.PROPERTIES.cql(),
                        property,
                        FullQualifiedName.class ) );
    }
}
