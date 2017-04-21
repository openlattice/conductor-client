package com.dataloom.data;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.SetMultimap;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;

public class EntitySetData implements Iterable<SetMultimap<FullQualifiedName, Object>> {

    private static final Logger                              logger = LoggerFactory
            .getLogger( EntitySetData.class );

    private Set<FullQualifiedName>                           authorizedPropertyFqns;
    private Iterable<SetMultimap<FullQualifiedName, Object>> entities;

    public EntitySetData(
            Set<FullQualifiedName> authorizedPropertyFqns,
            Iterable<SetMultimap<FullQualifiedName, Object>> entities ) {
        this.authorizedPropertyFqns = authorizedPropertyFqns;
        this.entities = entities;
    }

    public Set<FullQualifiedName> getAuthorizedPropertyFqns() {
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
