package com.kryptnostic.conductor.rpc;

import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.kryptnostic.conductor.rpc.odata.PropertyType;

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
			entity.addProperty( new Property( null, property.getTypename(), ValueType.PRIMITIVE, value ) );
		});
		return entity;
	}
	
	public static SetMultimap<FullQualifiedName, Object> mapRowToObject( Row row, Set<PropertyType> properties ) {
		SetMultimap<FullQualifiedName, Object> map = HashMultimap.create();
		properties.forEach(property -> {
			Object value = row.getObject( property.getTypename() );
			map.put( property.getFullQualifiedName(), value );
		});
		return map;
	}
}