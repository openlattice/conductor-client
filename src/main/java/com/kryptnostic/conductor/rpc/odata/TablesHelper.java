package com.kryptnostic.conductor.rpc.odata;

import java.util.EnumMap;

import com.dataloom.edm.internal.DatastoreConstants;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;

public class TablesHelper {
    static final EnumMap<Tables, CassandraTableBuilder> builders = new EnumMap<Tables, CassandraTableBuilder>(
            Tables.class );
    public static EnumMap<Tables, String>               keyspaces= new EnumMap<Tables, String>(
            Tables.class );;
    static {
        for ( Tables table : Tables.values() ) {
            keyspaces.put( table, DatastoreConstants.KEYSPACE );
            builders.put( table, Tables.getTableDefinition( table ) );
        }
    }
}
