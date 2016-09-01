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

/**
 @param Map<String, FullQualifiedName> A map that sends typename to FullQualifedName
 @return Lambda function transforming a Row to a SetMultimap, with keys being FullQualifiedName of column typename of Row, and value being the value in the Row.
 */
public final class ResultSetConversion {

	private void ResultSetConversion() {}

	/**
	 * // Only one static method here; should be incorporated in class that
	 * writes query result into Cassandra Table
	 * 
	 * @param Row
	 * @return SetMultiMap<FullQualifiedName, Object>
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