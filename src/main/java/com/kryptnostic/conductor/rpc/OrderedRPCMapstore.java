package com.kryptnostic.conductor.rpc;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class OrderedRPCMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<OrderedRPCKey, byte[]> {
    private static final Logger logger = LoggerFactory.getLogger( OrderedRPCMapstore.class );

    public OrderedRPCMapstore( Session session ) {
        super( HazelcastMap.RPC_DATA_ORDERED.name(), session, Table.RPC_DATA_ORDERED.getBuilder() );
    }

    @Override
    public OrderedRPCKey generateTestKey() {
        return new OrderedRPCKey( UUID.randomUUID(), 4.0 );
    }

    @Override
    public byte[] generateTestValue() {
        return "test value".getBytes();
    }

    @Override
    protected BoundStatement bind( OrderedRPCKey key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.RPC_REQUEST_ID.cql(), key.getRequestId() )
                .setDouble( CommonColumns.RPC_WEIGHT.cql(), key.getWeight() );
    }

    @Override
    protected BoundStatement bind( OrderedRPCKey key, byte[] value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.RPC_REQUEST_ID.cql(), key.getRequestId() )
                .setDouble( CommonColumns.RPC_WEIGHT.cql(), key.getWeight() )
                .setBytes( CommonColumns.RPC_VALUE.cql(), ByteBuffer.wrap( value ) );
    }

    @Override
    protected OrderedRPCKey mapKey( Row rs ) {
        return new OrderedRPCKey(
                rs.getUUID( CommonColumns.RPC_REQUEST_ID.cql() ),
                rs.getDouble( CommonColumns.RPC_WEIGHT.cql() ) );
    }

    @Override
    protected byte[] mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return row.getBytes( CommonColumns.RPC_VALUE.cql() ).array();
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public void store( OrderedRPCKey key, byte[] value ) {
        Futures.addCallback( asyncStore( key, value ), new FutureCallback() {

            @Override
            public void onSuccess( Object result ) {}

            @Override
            public void onFailure( Throwable t ) {
                logger.debug( "unable to save ordered rpc data" );
            }

        } );

    }

    @SuppressWarnings( "unchecked" )
    @Override
    public void storeAll( Map<OrderedRPCKey, byte[]> map ) {
        map.entrySet().parallelStream().forEach( entry -> {
            Futures.addCallback( asyncStore( entry.getKey(), entry.getValue() ), new FutureCallback() {

                @Override
                public void onSuccess( Object result ) {}

                @Override
                public void onFailure( Throwable t ) {
                    logger.debug( "unable to save ordered rpc data" );
                }

            } );
        } );
    }

}
