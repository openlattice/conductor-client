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

package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

public class QueryResult implements Serializable, Iterable<Row> {
	private static final String KEYSPACE   = "keyspace";
	private static final String TABLE_NAME = "tableName";
	private static final String QUERY_ID   = "queryId";
	private static final String SESSION_ID = "sessionId";
	private static final long serialVersionUID = -703400960761943382L;
	
    private final String			  keyspace;
    private final String              tableName;    
    private final UUID	              queryId;
    private final String              sessionId;
    private final Optional<Session>	  session;

    @JsonCreator
    public QueryResult(
            @JsonProperty( KEYSPACE ) String keyspace,
            @JsonProperty( TABLE_NAME ) String tableName,
            @JsonProperty( QUERY_ID ) UUID queryId,
            @JsonProperty( SESSION_ID ) String sessionId ) {
        this(keyspace, tableName, queryId, sessionId, Optional.absent());
    }

    public QueryResult(
    		String keyspace,
    		String tableName,
            UUID queryId,
            String sessionId,
            Optional<Session> session ) {
    	this.keyspace = keyspace;
    	this.tableName = tableName;
        this.queryId = queryId;
        this.sessionId = sessionId;
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

    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( keyspace == null ) ? 0 : keyspace.hashCode() );
        result = prime * result + ( ( queryId == null ) ? 0 : queryId.hashCode() );
        result = prime * result + ( ( session == null ) ? 0 : session.hashCode() );
        result = prime * result + ( ( sessionId == null ) ? 0 : sessionId.hashCode() );
        result = prime * result + ( ( tableName == null ) ? 0 : tableName.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( obj == null ) {
            return false;
        }
        if ( !( obj instanceof QueryResult ) ) {
            return false;
        }
        QueryResult other = (QueryResult) obj;
        if ( keyspace == null ) {
            if ( other.keyspace != null ) {
                return false;
            }
        } else if ( !keyspace.equals( other.keyspace ) ) {
            return false;
        }
        if ( queryId == null ) {
            if ( other.queryId != null ) {
                return false;
            }
        } else if ( !queryId.equals( other.queryId ) ) {
            return false;
        }
        if ( session == null ) {
            if ( other.session != null ) {
                return false;
            }
        } else if ( !session.equals( other.session ) ) {
            return false;
        }
        if ( sessionId == null ) {
            if ( other.sessionId != null ) {
                return false;
            }
        } else if ( !sessionId.equals( other.sessionId ) ) {
            return false;
        }
        if ( tableName == null ) {
            if ( other.tableName != null ) {
                return false;
            }
        } else if ( !tableName.equals( other.tableName ) ) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "QueryResult [keyspace=" + keyspace + ", tableName=" + tableName + ", queryId=" + queryId + ", sessionId=" + sessionId;
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
	
	public List<ColumnMetadata> getColumnData() {
		if ( session.isPresent() ) {
			return session
					.get()
					.getCluster()
					.getMetadata()
					.getKeyspace( keyspace )
					.getTable( tableName )
					.getColumns();
		}
		return ImmutableList.of();
	}
}
