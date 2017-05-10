package com.dataloom.hazelcast.serializers;

import com.dataloom.graph.core.objects.EdgeCountEntryProcessor;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class EdgeCountEntryProcessorStreamSerializerTest
        extends AbstractStreamSerializerTest<EdgeCountEntryProcessorStreamSerializer, EdgeCountEntryProcessor> {
    @Override protected EdgeCountEntryProcessorStreamSerializer createSerializer() {
        return new EdgeCountEntryProcessorStreamSerializer();
    }

    @Override protected EdgeCountEntryProcessor createInput() {
        return new EdgeCountEntryProcessor( UUID.randomUUID(),
                ImmutableSet.of( UUID.randomUUID(), UUID.randomUUID() ) );
    }
}
