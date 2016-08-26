package com.kryptnostic.datastore.cassandra;

import java.util.Arrays;
import java.util.function.Function;

import com.datastax.driver.core.DataType;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;


/**
 * This class is not thread safe.
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt; 
 *
 */
public class CassandraTableBuilder {
    private Optional<String>                  keyspace     = Optional.absent();
    private final String                      name;
    private boolean                           ifNotExists  = false;
    private CommonColumns[]                   partition    = null;
    private CommonColumns[]                   clustering   = new CommonColumns[] {};
    private CommonColumns[]                   columns      = new CommonColumns[] {};
    private Function<CommonColumns, DataType> typeResolver = c -> c.getType();

    public CassandraTableBuilder( String keyspace, String name ) {
        this( name );
        this.keyspace = Optional.of( keyspace );
    }

    public CassandraTableBuilder( String name ) {
        this.name = name;
    }

    public CassandraTableBuilder partitionKey( CommonColumns... columns ) {
        this.partition = Preconditions.checkNotNull( columns );
        Arrays.asList( columns ).forEach( Preconditions::checkNotNull );
        Preconditions.checkArgument( columns.length > 0, "Must specify at least one partition key column." );
        return this;
    }

    public CassandraTableBuilder clusteringColumns( CommonColumns... columns ) {
        this.clustering = Preconditions.checkNotNull( columns );
        Arrays.asList( columns ).forEach( Preconditions::checkNotNull );
        return this;
    }

    public CassandraTableBuilder columns( CommonColumns... columns ) {
        this.columns = Preconditions.checkNotNull( columns );
        Arrays.asList( columns ).forEach( Preconditions::checkNotNull );
        return this;
    }

    public CassandraTableBuilder ifNotExists() {
        this.ifNotExists = true;
        return this;
    }

    public CassandraTableBuilder withTypeResolver( Function<CommonColumns, DataType> typeResolver ) {
        this.typeResolver = typeResolver;
        return this;
    }

    public String buildQuery() {
        Preconditions.checkState( partition != null, "Partition key was not configured" );
        StringBuilder query = new StringBuilder( "CREATE TABLE " );
        if ( keyspace.isPresent() ) {
            query.append( keyspace.get() ).append( "." );
        }

        query.append( name );

        if ( ifNotExists ) {
            query.append( " IF NOT EXISTS" );
        }

        query.append( " ( " );
        appendColumnDefs( query, partition );
        if ( clustering.length > 0 ) {
            appendColumnDefs( query, clustering );
        }
        if ( columns.length > 0 ) {
            appendColumnDefs( query, columns );
        }

        // extra comma from appendColumns is already included
        query.append( " PRIMARY KEY (" );

        //
        if ( this.partition.length > 1 ) {
            query.append( " ( " );
        }

        query.append( getPrimaryKeyDef( partition ) );

        //
        if ( this.partition.length > 1 ) {
            query.append( " ) " );
        }

        if ( clustering.length > 0 ) {
            query.append( ", " );
            query.append( getPrimaryKeyDef( clustering ) );
        }

        query.append( " ) )" );
        return query.toString();
    }

    private static String getPrimaryKeyDef( CommonColumns[] columns ) {
        StringBuilder builder = new StringBuilder();

        int len = columns.length - 1;
        for ( int i = 0; i < len; ++i ) {
            builder
                    .append( columns[ i ].toString() ).append( "," );
        }
        builder
                .append( columns[ len ].toString() );

        return builder.toString();
    }

    private StringBuilder appendColumnDefs(
            StringBuilder builder,
            CommonColumns[] columns ) {
        for ( int i = 0; i < columns.length; ++i ) {
            builder
                    .append( columns[ i ].toString() )
                    .append( " " )
                    .append( columns[ i ].getType( typeResolver ).toString() )
                    .append( "," );
        }
        return builder;
    }
}
