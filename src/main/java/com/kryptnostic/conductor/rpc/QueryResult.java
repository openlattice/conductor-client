package com.kryptnostic.conductor.rpc;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.base.Optional;

public class QueryResult implements Serializable, Iterable<Row> {
	private static final String KEYSPACE   = "keyspace";
	private static final String TABLE_NAME = "tableName";
	private static final String QUERY_ID   = "queryId";
	private static final String SESSION_ID = "sessionId";
	private static final String ES         = "es";
	private static final long serialVersionUID = -703400960761943382L;

	public static class QueryResultCsvReader {
        private static final transient CsvMapper mapper = new CsvMapper();
        private static final transient CsvSchema schema = mapper.schemaFor( QueryResult.class );
        private static final Logger              logger = LoggerFactory.getLogger( QueryResultCsvReader.class );

        static {
            logger.info( "Schema: {}", schema.getColumnDesc() );
        }

        public static QueryResult getQueryResult( String row ) throws IOException {
            try {
                return mapper.reader( QueryResult.class ).with( schema ).readValue( row );
            } catch ( IOException e ) {
                logger.error( "Something went wrong parsing row: {}", row, e );
                throw e;
            }
        }
	}
	
    private final String			  keyspace;
    private final String              tableName;    
    private final UUID	              queryId;
    private final String              sessionId;
    private final CsdlEntitySet       es;
    private final Optional<Session>	  session;

    @JsonCreator
    public QueryResult(
            @JsonProperty( KEYSPACE ) String keyspace,
            @JsonProperty( TABLE_NAME ) String tableName,
            @JsonProperty( QUERY_ID ) UUID queryId,
            @JsonProperty( SESSION_ID ) String sessionId,
            @JsonProperty( ES ) CsdlEntitySet es ) {
        this(keyspace, tableName, queryId, sessionId, es, Optional.absent());
    }

    public QueryResult(
    		String keyspace,
    		String tableName,
            UUID queryId,
            String sessionId,
            CsdlEntitySet es,
            Optional<Session> session ) {
    	this.keyspace = keyspace;
    	this.tableName = tableName;
        this.queryId = queryId;
        this.sessionId = sessionId;
        this.es = es;
        this.session = session;
    }
    
    @JsonProperty( KEYSPACE )
    public String getKeyspace() {
    	return keyspace;
    }
    
    @JsonProperty( TABLE_NAME )
    public String getTableName() {
        return tableName;
    }

    @JsonProperty( QUERY_ID )
    public UUID getQueryId() {
        return queryId;
    }

    @JsonProperty( SESSION_ID )
    public String getSessionId() {
        return sessionId;
    }

    @JsonProperty( ES )
    public CsdlEntitySet getEntitySet() {
        return es;
    }

    @Override
    public String toString() {
        return "QueryResult [keyspace=" + keyspace + ", tableName=" + tableName + ", queryId=" + queryId + ", sessionId=" + sessionId + ", es=" + es + "]";
    }

	@Override
	public Iterator<Row> iterator() {
		if ( session.isPresent() ) {
			Statement statement = QueryBuilder.select().from( keyspace, tableName );
			ResultSet rs = session.get().execute( statement );
			return rs.iterator();
		}
		return Collections.emptyIterator();
	}
}
