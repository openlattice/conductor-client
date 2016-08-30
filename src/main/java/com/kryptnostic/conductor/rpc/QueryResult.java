package com.kryptnostic.conductor.rpc;

import java.io.IOException;
import java.io.Serializable;
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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.base.Optional;

@JsonPropertyOrder( { "tableName", "queryId", "sessionId", "es" } )
public class QueryResult implements Serializable, Iterable<Row> {
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
	
    private final String              tableName;
    private final UUID	              queryId;
    private final String              sessionId;
    private final CsdlEntitySet       es;
    private final Optional<Session>	  session;

    @JsonCreator
    public QueryResult(
            @JsonProperty( "tableName" ) String tableName,
            @JsonProperty( "queryId" ) UUID queryId,
            @JsonProperty( "sessionId" ) String sessionId,
            @JsonProperty( "es" ) CsdlEntitySet es ) {
        this(tableName, queryId, sessionId, es, Optional.absent());
    }

    public QueryResult(
    		String tableName,
            UUID queryId,
            String sessionId,
            CsdlEntitySet es,
            Optional<Session> session ) {
    	this.tableName = tableName;
        this.queryId = queryId;
        this.sessionId = sessionId;
        this.es = es;
        this.session = session;
    }

    @JsonProperty( "tableName" )
    public String getTableName() {
        return tableName;
    }

    @JsonProperty( "queryId" )
    public UUID getQueryId() {
        return queryId;
    }

    @JsonProperty( "sessionId" )
    public String getSessionId() {
        return sessionId;
    }

    @JsonProperty( "es" )
    public CsdlEntitySet getEntitySet() {
        return es;
    }

    @Override
    public String toString() {
        return "QueryResult [tableName=" + tableName + ", queryId=" + queryId + ", sessionId=" + sessionId + ", es=" + es + "]";
    }

	@Override
	public Iterator<Row> iterator() {
		if ( session.isPresent() ) {
			Statement statement = QueryBuilder.select( "*" ).from( tableName );
			ResultSet rs = session.get().execute( statement );
			return rs.iterator();
		}
		return null;
	}
}
