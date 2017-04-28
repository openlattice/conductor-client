package com.dataloom.data;

import java.util.Iterator;
import java.util.Set;

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

    private Set<T>                           authorizedPropertyFqns;
    private Iterable<SetMultimap<T, Object>> entities;

    public EntitySetData(
            Set<T> authorizedPropertyFqns,
            Iterable<SetMultimap<T, Object>> entities ) {
        this.authorizedPropertyFqns = authorizedPropertyFqns;
        this.entities = entities;
    }

    public Set<T> getAuthorizedPropertyFqns() {
        return authorizedPropertyFqns;
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
