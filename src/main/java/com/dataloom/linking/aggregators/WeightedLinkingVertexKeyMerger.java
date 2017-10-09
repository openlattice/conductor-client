package com.dataloom.linking.aggregators;

import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.linking.WeightedLinkingVertexKey;
import com.dataloom.linking.WeightedLinkingVertexKeySet;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractMerger;

public class WeightedLinkingVertexKeyMerger extends
        AbstractMerger<LinkingVertexKey, WeightedLinkingVertexKeySet, WeightedLinkingVertexKey> {
    private Iterable<WeightedLinkingVertexKey> objects;

    public WeightedLinkingVertexKeyMerger( Iterable<WeightedLinkingVertexKey> objects ) {
        super( objects );
        this.objects = objects;
    }

    @Override protected WeightedLinkingVertexKeySet newEmptyCollection() {
        return new WeightedLinkingVertexKeySet();
    }

    public Iterable<WeightedLinkingVertexKey> getObjects() {
        return objects;
    }
}
