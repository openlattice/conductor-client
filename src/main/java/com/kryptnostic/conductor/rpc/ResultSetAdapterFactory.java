package com.kryptnostic.conductor.rpc;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.requests.Action;
import com.dataloom.authorization.requests.PermissionsInfo;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.authorization.requests.PrincipalType;
import com.dataloom.authorization.requests.PropertyTypeInEntitySetAclRequestWithRequestingUser;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.requests.PropertyTypeInEntitySetAclRequest;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.joda.LocalDateCodec;
import com.datastax.driver.extras.codecs.joda.LocalTimeCodec;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.HashMultimap;

import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;

import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;
import com.kryptnostic.conductor.codecs.TimestampDateTimeTypeCodec;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import com.kryptnostic.datastore.cassandra.CommonColumns;

public final class ResultSetAdapterFactory {
    
    private ResultSetAdapterFactory() {}

    private static final Map<DataType.Name, TypeCodec> preferredCodec = new HashMap<>();

    static {
        preferredCodec.put( DataType.Name.TIMESTAMP, TimestampDateTimeTypeCodec.getInstance() );
    }
    
	/**
	 * // Only one static method here; should be incorporated in class that
	 * writes query result into Cassandra Table
	 * 
	 * @param mapTypenameToFullQualifiedName a Map that sends Typename to FullQualifiedName
	 * @return a Function that converts Row to SetMultimap, which has FullQualifiedName as a key and Row value as the corresponding value.
	 */
	public static Function< Row, SetMultimap<FullQualifiedName, Object> > toSetMultimap(
			final Map<String, FullQualifiedName> mapTypenameToFullQualifiedName) {
		return (Row row) -> {			
            List<ColumnDefinitions.Definition> definitions = row.getColumnDefinitions().asList();
            int numOfColumns = definitions.size();

            return IntStream.range(0, numOfColumns).boxed().collect(
			        Collector.of(
			                ImmutableSetMultimap.Builder<FullQualifiedName, Object>::new,
			                ( builder, index ) -> builder.put( mapTypenameToFullQualifiedName.get( definitions.get(index).getName() ) , row.getObject(index) ),
			                ( lhs, rhs ) -> lhs.putAll( rhs.build() ),
			                builder -> builder.build()
			                )
			        );
		};
	}
	
	public static Entity mapRowToEntity( Row row, Set<PropertyType> properties ) {
		Entity entity = new Entity();
		properties.forEach(property -> {
			Object value = row.getObject( property.getTypename() );
			entity.addProperty( new Property( property.getFullQualifiedName().getFullQualifiedNameAsString(), property.getName(), ValueType.PRIMITIVE, value ) );
		});
		return entity;
	}
	
	public static SetMultimap<FullQualifiedName, Object> mapRowToObject( Row row, Collection<PropertyType> properties ) {
		SetMultimap<FullQualifiedName, Object> map = HashMultimap.create();
		properties.forEach(property -> {
			Object value = getObject( row, CassandraEdmMapping.getCassandraType( property.getDatatype() ), "value_" + property.getTypename() );
			map.put( property.getFullQualifiedName(), value );
		});
		return map;
	}
	
    private static Object getObject( Row row, DataType dt, String colName ) {
        Name dtName = dt.getName();
        if ( preferredCodec.containsKey( dtName ) ) {
            return row.get( colName, preferredCodec.get( dtName ) );
        } else {
            return row.getObject( colName );
        }
    }

	/**
	 * 
	 * @param row Cassandra Row object, expected to have a single column of UUID
	 * @return UUID
	 */
	public static UUID mapRowToUUID( Row row) {
		return row.getUUID(0);
	}
	
    public static PermissionsInfo mapRoleRowToPermissionsInfo( Row row ) {
        return new PermissionsInfo()
                .setPrincipal( new Principal( PrincipalType.ROLE ).setName( row.getString( CommonColumns.ROLE.cql() ) ))
                .setPermissions( row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() ) );
    }
    
    public static PermissionsInfo mapUserRowToPermissionsInfo( Row row ) {
        String userId = row.getString( CommonColumns.USER.cql() );
        return new PermissionsInfo()
                .setPrincipal( new Principal( PrincipalType.USER ).setId( userId ) )
                .setPermissions( row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() ) );
    }
    
    public static PropertyTypeInEntitySetAclRequestWithRequestingUser mapRowToPropertyTypeInEntitySetAclRequestWithRequestingUser( PrincipalType type, Row row ){
        Principal principal = new Principal( type );
        switch( type ){
            case ROLE:
                principal = principal.setName( row.getString( CommonColumns.NAME.cql() ) );
                break;
            case USER:
                String userId = row.getString( CommonColumns.USERID.cql() );
                principal = principal.setId( userId );
                break;
            default:
        }
        
        PropertyTypeInEntitySetAclRequest request = new PropertyTypeInEntitySetAclRequest()
                .setPrincipal( principal )
                .setAction( Action.REQUEST )
                .setName( row.getString( CommonColumns.ENTITY_SET.cql() ) )
                .setPropertyType( row.get( CommonColumns.PROPERTY_TYPE.cql(), FullQualifiedName.class ))
                .setPermissions( row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() ))
                .setTimestamp( row.get( CommonColumns.CLOCK.cql(), Instant.class ).toString() )
                .setRequestId( row.getUUID( CommonColumns.REQUESTID.cql() ) );
        String requestingUser = row.getString( CommonColumns.USER.cql() );
        return new PropertyTypeInEntitySetAclRequestWithRequestingUser().setRequest( request ).setRequestingUser( requestingUser );
    }
}