package com.dataloom.edm.schemas.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.schemas.SchemaQueryService;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;

public class CassandraSchemaQueryService implements SchemaQueryService {
    private final Session           session;
    private final PreparedStatement propertyTypesInSchemaQuery;
    private final PreparedStatement entityTypesInSchemaQuery;
    private final RegularStatement  getNamespaces;

    public CassandraSchemaQueryService( String keyspace, Session session ) {
        this.session = checkNotNull( session, "Session cannot be null." );
        propertyTypesInSchemaQuery = session.prepare( getPropertyTypesInSchema( keyspace ) );
        entityTypesInSchemaQuery = session.prepare( getEntityTypesInSchema( keyspace ) );
        getNamespaces = QueryBuilder.select( CommonColumns.NAMESPACE.cql() ).distinct()
                .from( Tables.SCHEMAS.getKeyspace(), Tables.SCHEMAS.getName() );
    }

    private static RegularStatement getPropertyTypesInSchema( String keyspace ) {
        return QueryBuilder
                .select( CommonColumns.ID.cql() )
                .from( keyspace, Tables.PROPERTY_TYPES.getName() ).allowFiltering()
                .where( QueryBuilder.contains( CommonColumns.SCHEMAS.cql(), CommonColumns.SCHEMAS.bindMarker() ) );
    }

    private static RegularStatement getEntityTypesInSchema( String keyspace ) {
        return QueryBuilder
                .select( CommonColumns.ID.cql() )
                .from( keyspace, Tables.ENTITY_TYPES.getName() ).allowFiltering()
                .where( QueryBuilder.contains( CommonColumns.SCHEMAS.cql(), CommonColumns.SCHEMAS.bindMarker() ) );
    }

    /*
     * (non-Javadoc)
     * @see
     * com.dataloom.edm.schemas.cassandra.SchemaQueryService#getAllPropertyTypesInSchema(org.apache.olingo.commons.api.
     * edm.FullQualifiedName)
     */
    @Override
    public Set<UUID> getAllPropertyTypesInSchema( FullQualifiedName schemaName ) {
        ResultSet propertyTypes = session.execute(
                propertyTypesInSchemaQuery.bind()
                        .set( CommonColumns.SCHEMAS.cql(), schemaName, FullQualifiedName.class ) );
        return ImmutableSet.copyOf( Iterables.transform( propertyTypes, RowAdapters::id ) );
    }

    /*
     * (non-Javadoc)
     * @see
     * com.dataloom.edm.schemas.cassandra.SchemaQueryService#getAllEntityTypesInSchema(org.apache.olingo.commons.api.edm
     * .FullQualifiedName)
     */
    @Override
    public Set<UUID> getAllEntityTypesInSchema( FullQualifiedName schemaName ) {
        ResultSet propertyTypes = session.execute(
                entityTypesInSchemaQuery.bind()
                        .set( CommonColumns.SCHEMAS.cql(), schemaName, FullQualifiedName.class ) );
        return ImmutableSet.copyOf( Iterables.transform( propertyTypes, RowAdapters::id ) );
    }

    /*
     * (non-Javadoc)
     * @see com.dataloom.edm.schemas.SchemaQueryService#getNamespaces()
     */
    @Override
    public Iterable<String> getNamespaces() {
        return Iterables.transform( session.execute( getNamespaces ), RowAdapters::namespace );
    }
}
