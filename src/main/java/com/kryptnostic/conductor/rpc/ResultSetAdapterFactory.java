package com.kryptnostic.conductor.rpc;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.HashMultimap; 

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;

import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.PermissionsInfo;
import com.kryptnostic.datastore.Principal;
import com.kryptnostic.datastore.PrincipalType;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.services.requests.Action;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntitySetAclRequest;

public final class ResultSetAdapterFactory {

	private void ResultSetAdapterFactory() {}

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
			Object value = row.getObject( "value_" + property.getTypename() );
			map.put( property.getFullQualifiedName(), value );
		});
		return map;
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
                .setPrincipal( new Principal( PrincipalType.ROLE, row.getString( CommonColumns.ROLE.cql() ) ))
                .setPermissions( row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() ) );
    }
    
    public static PermissionsInfo mapUserRowToPermissionsInfo( Row row ) {
        return new PermissionsInfo()
                .setPrincipal( new Principal( PrincipalType.USER, row.getString( CommonColumns.USER.cql() ) ))
                .setPermissions( row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() ) );
    }
    
    public static PropertyTypeInEntitySetAclRequest mapRowToPropertyTypeInEntitySetAclRequest( PrincipalType type, Row row ){
        return new PropertyTypeInEntitySetAclRequest()
                .setPrincipal( new Principal( type, row.getString( CommonColumns.NAME.cql() ) ) )
                .setAction( Action.REQUEST )
                .setName( row.getString( CommonColumns.ENTITY_SET.cql() ) )
                .setPropertyType( row.get( CommonColumns.PROPERTY_TYPE.cql(), FullQualifiedName.class ))
                .setPermissions( row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() ))
                .setTimestamp( row.get( CommonColumns.CLOCK.cql(), Instant.class ).toString() )
                .setRequestId( row.getUUID( CommonColumns.REQUESTID.cql() ) );
    }
}