package com.dataloom.hazelcast.serializers;

import com.dataloom.graph.core.Neighborhood;
import com.dataloom.graph.mapstores.EdgesMapstore;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class NeighborhoodStreamSerializerTest extends
        AbstractStreamSerializerTest<NeighborhoodStreamSerializer, Neighborhood> {

    @Override protected NeighborhoodStreamSerializer createSerializer() {
        return new NeighborhoodStreamSerializer();
    }

    @Override protected Neighborhood createInput() {
        return Neighborhood.randomNeighborhood();
    }
}
