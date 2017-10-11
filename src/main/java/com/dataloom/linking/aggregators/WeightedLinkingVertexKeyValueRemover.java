package com.dataloom.linking.aggregators;

import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.linking.WeightedLinkingVertexKey;
import com.dataloom.linking.WeightedLinkingVertexKeySet;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover;

public class WeightedLinkingVertexKeyValueRemover extends AbstractRemover<LinkingVertexKey, WeightedLinkingVertexKeySet, WeightedLinkingVertexKey> {
    private Iterable<WeightedLinkingVertexKey> objects;

    public WeightedLinkingVertexKeyValueRemover( Iterable<WeightedLinkingVertexKey> objectsToRemove ) {
        super( objectsToRemove );
        this.objects = objectsToRemove;
    }

    public Iterable<WeightedLinkingVertexKey> getObjects() {
        return objects;
    }

}
