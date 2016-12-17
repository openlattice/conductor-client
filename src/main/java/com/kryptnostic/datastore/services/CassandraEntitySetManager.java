package com.kryptnostic.datastore.services;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.auth0.jwt.internal.org.apache.commons.lang3.StringUtils;
import com.clearspring.analytics.util.Preconditions;
import com.dataloom.edm.internal.EntitySet;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.Iterables;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.Queries;
import com.kryptnostic.datastore.cassandra.RowAdapters;

public class CassandraEntitySetManager {
    private final Session           session;
    private final String            keyspace;

    private final PreparedStatement getEntitySetsByType;
    private final Select            getAllEntitySets;

    public CassandraEntitySetManager( Session session, String keyspace ) {
        Preconditions.checkArgument( StringUtils.isNotBlank( keyspace ), "Keyspace cannot be blank." );
        this.session = session;
        this.keyspace = keyspace;
        this.getEntitySetsByType = session
                .prepare( QueryBuilder.select().from( this.keyspace, Tables.ENTITY_SETS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.TYPE.cql(), CommonColumns.TYPE.bindMarker() ) ) );
        this.getAllEntitySets = QueryBuilder.select().all().from( keyspace, Tables.ENTITY_SETS.getName() );
        createEntitySetsTableIfNotExists( keyspace, session );
    }

    public Iterable<EntitySet> getAllEntitySetsForType( FullQualifiedName type ) {
        ResultSetFuture rsf = session.executeAsync(
                getEntitySetsByType.bind().set( CommonColumns.TYPE.cql(), type, FullQualifiedName.class ) );
        return Iterables.transform( rsf.getUninterruptibly(), RowAdapters::entitySet );
    }

    public Iterable<EntitySet> getAllEntitySets() {
        ResultSetFuture rsf = session.executeAsync( getAllEntitySets );
        return Iterables.transform( rsf.getUninterruptibly(), RowAdapters::entitySet );
    }

    private static void createEntitySetsTableIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.getCreateEntitySetsTableQuery( keyspace ) );
        session.execute( Queries.CREATE_INDEX_ON_NAME );
    }

}
