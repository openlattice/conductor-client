package com.dataloom.data;

import java.util.Iterator;
import java.util.LinkedHashSet;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.SetMultimap;

public class EntitySetData implements Iterable<SetMultimap<FullQualifiedName, Object>> {

    private static final Logger                              logger = LoggerFactory
            .getLogger( EntitySetData.class );

    private LinkedHashSet<FullQualifiedName>                           authorizedPropertyFqns;
    private Iterable<SetMultimap<FullQualifiedName, Object>> entities;

    public EntitySetData(
            LinkedHashSet<FullQualifiedName> authorizedPropertyFqns,
            Iterable<SetMultimap<FullQualifiedName, Object>> entities ) {
        this.authorizedPropertyFqns = authorizedPropertyFqns;
        this.entities = entities;
    }

    public LinkedHashSet<FullQualifiedName> getAuthorizedPropertyFqns() {
        return authorizedPropertyFqns;
    }

    @JsonValue
    public Iterable<SetMultimap<FullQualifiedName, Object>> getEntities() {
        return entities::iterator;
    }

    @Override
    public Iterator<SetMultimap<FullQualifiedName, Object>> iterator() {
        throw new NotImplementedException( "Purposefully not implemented." );
    }
}
