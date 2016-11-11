package com.kryptnostic.datastore.cassandra;

import java.util.Arrays;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.DataType;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.kryptnostic.conductor.rpc.odata.Tables;

/**
 * This class is not thread safe.
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 *
 */
public class CassandraTableBuilder {

    public static class ValueColumn {
        private final String   cql;
        private final DataType dataType;

        public ValueColumn( String cql, DataType dataType ) {
            this.cql = cql;
            this.dataType = dataType;
        }

        public String cql() {
            return cql;
        }

        public DataType getDataType() {
            return dataType;
        }
    }

    private Optional<String>                  keyspace               = Optional.absent();
    private final String                      name;
    private boolean                           ifNotExists            = false;
    private CommonColumns[]                   partition              = null;
    private CommonColumns[]                   clustering             = new CommonColumns[] {};
    private ValueColumn[]                     clusteringValueColumns = new ValueColumn[] {};
    private CommonColumns[]                   columns                = new CommonColumns[] {};
    private ValueColumn[]                     valueColumns           = new ValueColumn[] {};
    private Function<CommonColumns, DataType> typeResolver           = c -> c.getType();

    public CassandraTableBuilder( String keyspace, Tables name ) {
        this( name.getTableName() );
        this.keyspace = Optional.of( keyspace );
    }

    public CassandraTableBuilder( String keyspace, String name ) {
        this( name );
        this.keyspace = Optional.of( keyspace );
    }

    public CassandraTableBuilder( String name ) {
        Preconditions.checkArgument( StringUtils.isNotBlank( name ), "Table name cannot be blank." );
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

    public CassandraTableBuilder clusteringColumns( ValueColumn... clusteringValueColumns ) {
        this.clusteringValueColumns = Preconditions.checkNotNull( clusteringValueColumns );
        Arrays.asList( clusteringValueColumns ).forEach( Preconditions::checkNotNull );
        return this;
    }

    public CassandraTableBuilder columns( CommonColumns... columns ) {
        this.columns = Preconditions.checkNotNull( columns );
        Arrays.asList( columns ).forEach( Preconditions::checkNotNull );
        return this;
    }

    public CassandraTableBuilder columns( ValueColumn... valueColumns ) {
        this.valueColumns = Preconditions.checkNotNull( valueColumns );
        Arrays.asList( valueColumns ).forEach( Preconditions::checkNotNull );
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

        if ( ifNotExists ) {
            query.append( "IF NOT EXISTS " );
        }

        if ( keyspace.isPresent() ) {
            query.append( keyspace.get() ).append( "." );
        }

        query.append( name );

        query.append( " ( " );
        appendColumnDefs( query, partition );
        if ( clustering.length > 0 ) {
            appendColumnDefs( query, clustering );
        }

        if ( clusteringValueColumns.length > 0 ) {
            appendColumnDefs( query, clusteringValueColumns );
        }

        if ( columns.length > 0 ) {
            appendColumnDefs( query, columns );
        }
        if ( valueColumns.length > 0 ) {
            appendColumnDefs( query, valueColumns );
        }

        // extra comma from appendColumns is already included
        query.append( " PRIMARY KEY (" );

        // Only add if compound partition key
        if ( this.partition.length > 1 ) {
            query.append( " ( " );
        }

        query.append( getPrimaryKeyDef( partition ) );

        // Only add if compound partition key
        if ( this.partition.length > 1 ) {
            query.append( " ) " );
        }

        if ( clustering.length > 0 ) {
            query.append( ", " );
            query.append( getPrimaryKeyDef( clustering ) );
        }

        if ( clusteringValueColumns.length > 0 ) {
            query.append( ", " );
            query.append( getPrimaryKeyDef( clusteringValueColumns ) );
        }

        query.append( " ) )" );
        return query.toString();
    }

    private static String getPrimaryKeyDef( CommonColumns[] columns ) {
        StringBuilder builder = new StringBuilder();

        int len = columns.length - 1;
        for ( int i = 0; i < len; ++i ) {
            builder
                    .append( columns[ i ].cql() ).append( "," );
        }
        builder
                .append( columns[ len ].cql() );

        return builder.toString();
    }

    private static String getPrimaryKeyDef( ValueColumn[] columns ) {
        StringBuilder builder = new StringBuilder();

        int len = columns.length - 1;
        for ( int i = 0; i < len; ++i ) {
            builder
                    .append( columns[ i ].cql() ).append( "," );
        }
        builder
                .append( columns[ len ].cql() );

        return builder.toString();
    }

    private StringBuilder appendColumnDefs(
            StringBuilder builder,
            CommonColumns[] columns ) {
        for ( int i = 0; i < columns.length; ++i ) {
            builder
                    .append( columns[ i ].cql() )
                    .append( " " )
                    .append( columns[ i ].getType( typeResolver ).toString() )
                    .append( "," );
        }
        return builder;
    }

    private StringBuilder appendColumnDefs(
            StringBuilder builder,
            ValueColumn[] columns ) {
        for ( int i = 0; i < columns.length; ++i ) {
            builder
                    .append( columns[ i ].cql() )
                    .append( " " )
                    .append( columns[ i ].getDataType().toString() )
                    .append( "," );
        }
        return builder;
    }

}
