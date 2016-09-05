package com.kryptnostic.conductor.rpc;

import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

public final class ResultSetAdapterFactory {

	private void ResultSetConversion() {}

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
}