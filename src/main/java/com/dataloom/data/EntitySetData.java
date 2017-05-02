package com.dataloom.data;

import java.util.Iterator;
import java.util.LinkedHashSet;

import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.SetMultimap;

/* 
 * Note: T must have a good .toString() method, as this will be used for serialization
 */
public class EntitySetData<T> implements Iterable<SetMultimap<T, Object>> {

    private static final Logger              logger = LoggerFactory
            .getLogger( EntitySetData.class );

    private LinkedHashSet<T>                 columnTitles;
    private Iterable<SetMultimap<T, Object>> entities;

    public EntitySetData(
            LinkedHashSet<T> columnTitles,
            Iterable<SetMultimap<T, Object>> entities ) {
        this.columnTitles = columnTitles;
        this.entities = entities;
    }

    public LinkedHashSet<T> getColumnTitles() {
        return columnTitles;
    }

    @JsonValue
    public Iterable<SetMultimap<T, Object>> getEntities() {
        return entities::iterator;
    }

    @Override
    public Iterator<SetMultimap<T, Object>> iterator() {
        throw new NotImplementedException( "Purposefully not implemented." );
    }
}
