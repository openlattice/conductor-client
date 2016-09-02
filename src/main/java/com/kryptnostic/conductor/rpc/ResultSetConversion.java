package com.kryptnostic.conductor.rpc;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.ColumnDefinitions;

import com.google.common.collect.Multimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Iterables;

public final class ResultSetConversion {

	private void ResultSetConversion() {}

	/**
	 * // Only one static method here; should be incorporated in class that
	 * writes query result into Cassandra Table
	 * 
	 * @param mapTypenameToFullQualifiedName a Map that sends Typename to FullQualifiedName
	 * @return a Function that converts Row to SetMultiMap, which has FullQualifiedName as a key and Row value as the corresponding value.
	 */
	public static Function< Row, SetMultimap<FullQualifiedName, Object> > toSetMultimap(
			final Map<String, FullQualifiedName> mapTypenameToFullQualifiedName) {
		return (Row row) -> {
			List<ColumnDefinitions.Definition> definitions = row.getColumnDefinitions().asList();
			int numOfColumns = definitions.size();
			SetMultimap<FullQualifiedName, Object> convertedData = HashMultimap.create();

			for (int i = 0; i < numOfColumns; i++) {
				FullQualifiedName name = mapTypenameToFullQualifiedName.get(definitions.get(i).getName());
				convertedData.put(name, row.getObject(i));
			}

			return convertedData;
		};
	}
}