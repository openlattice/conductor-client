package com.dataloom.linking;

import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListSet;

public class WeightedLinkingVertexKeySet extends ConcurrentSkipListSet<WeightedLinkingVertexKey> {

    public Collection<WeightedLinkingVertexKey> getValue() {
        return this;
    }
}
