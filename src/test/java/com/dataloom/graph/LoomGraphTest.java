package com.dataloom.graph;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import org.junit.Assert;
import org.junit.Test;

import com.dataloom.authorization.HzAuthzTest;
import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.GraphQueryService;
import com.dataloom.graph.core.LoomGraph;
import com.dataloom.graph.core.objects.LoomVertexKey;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;

public class LoomGraphTest extends HzAuthzTest {

    protected static final LoomGraph         lg;
    protected static final GraphQueryService gqs;

    static {
        gqs = new GraphQueryService( session );
        lg = new LoomGraph( gqs, hazelcastInstance );
    }

//    private LoomVertexKey createVertex() {
//        return getOrCreateVertex( TestDataFactory.entityKey() );
//    }

//    private LoomVertexKey getOrCreateVertex( EntityKey entityKey ) {
//        LoomVertexKey v = lg.getOrCreateVertex( entityKey );
//        // verification
//        Assert.assertNotNull( v );
//        Assert.assertEquals( v, lg.getVertexByEntityKey( entityKey ) );
//        Assert.assertEquals( v, lg.getVertexById( v.getKey() ) );
//
//        return v;
//    }
//
//    @Test
//    public void testCreateVertex() {
//        createVertex();
//    }
//
//    @Test
//    public void testCreateVertexAsync() {
//        EntityKey key1 = TestDataFactory.entityKey();
//        EntityKey key2 = TestDataFactory.entityKey();
//
//        LoomVertexFuture v1Async = lg.getOrCreateVertexAsync( key1 );
//        LoomVertexFuture v2Async = lg.getOrCreateVertexAsync( key2 );
//
//        LoomVertexKey v1 = v1Async.get();
//        LoomVertexKey v2 = v2Async.get();
//
//        // verification
//        Assert.assertEquals( v1, lg.getVertexByEntityKey( key1 ) );
//        Assert.assertEquals( v1, lg.getVertexById( v1.getKey() ) );
//        Assert.assertEquals( v2, lg.getVertexByEntityKey( key2 ) );
//        Assert.assertEquals( v2, lg.getVertexById( v2.getKey() ) );
//    }
//
//    @Test
//    public void testCreateEdge() {
//        LoomVertexKey v1 = createVertex();
//        LoomVertexKey v2 = createVertex();
//        EntityKey label = TestDataFactory.entityKey();
//
//        lg.addEdge( v1, v2, label );
//        LoomEdge edge = lg.getEdge( new EdgeKey( v1.getKey(), v2.getKey(), label ) );
//
//        // verification
//        Assert.assertNotNull( edge );
//        Assert.assertEquals( v1.getKey(), edge.getKey().getSrcId() );
//        Assert.assertEquals( v1.getReference().getEntitySetId(), edge.getSrcType() );
//        Assert.assertEquals( v2.getKey(), edge.getKey().getDstId() );
//        Assert.assertEquals( v2.getReference().getEntitySetId(), edge.getDstType() );
//        Assert.assertEquals( label, edge.getReference() );
//    }
//
//    @Test
//    public void testGetEdges() {
//        LoomVertexKey vA = createVertex();
//        LoomVertexKey vB = createVertex();
//        LoomVertexKey vC = createVertex();
//
//        UUID edgeType1 = UUID.randomUUID();
//        UUID edgeType2;
//        do {
//            edgeType2 = UUID.randomUUID();
//        } while ( edgeType2 == edgeType1 );
//
//        EntityKey labelAB = TestDataFactory.entityKey( edgeType1 );
//        EntityKey labelBC = TestDataFactory.entityKey( edgeType1 );
//        EntityKey labelAC = TestDataFactory.entityKey( edgeType1 );
//        EntityKey labelCA = TestDataFactory.entityKey( edgeType2 );
//
//        lg.addEdge( vA, vB, labelAB );
//        lg.addEdge( vB, vC, labelBC );
//        lg.addEdge( vA, vC, labelAC );
//        lg.addEdge( vC, vA, labelCA );
//
//        EdgeSelection srcIdOnly = new EdgeSelection(
//                Optional.of( vA.getKey() ),
//                Optional.absent(),
//                Optional.absent(),
//                Optional.absent(),
//                Optional.absent() );
//        Assert.assertEquals( 2, Iterables.size( lg.getEdges( srcIdOnly ) ) );
//
//        EdgeSelection srcTypeOnly = new EdgeSelection(
//                Optional.absent(),
//                Optional.of( vA.getReference().getEntitySetId() ),
//                Optional.absent(),
//                Optional.absent(),
//                Optional.absent() );
//        Assert.assertEquals( 2, Iterables.size( lg.getEdges( srcTypeOnly ) ) );
//
//        EdgeSelection dstIdOnly = new EdgeSelection(
//                Optional.absent(),
//                Optional.absent(),
//                Optional.of( vB.getKey() ),
//                Optional.absent(),
//                Optional.absent() );
//        Assert.assertEquals( 1, Iterables.size( lg.getEdges( dstIdOnly ) ) );
//
//        EdgeSelection dstTypeOnly = new EdgeSelection(
//                Optional.absent(),
//                Optional.absent(),
//                Optional.absent(),
//                Optional.of( vC.getReference().getEntitySetId() ),
//                Optional.absent() );
//        Assert.assertEquals( 2, Iterables.size( lg.getEdges( dstTypeOnly ) ) );
//
//        EdgeSelection edgeTypeOnly = new EdgeSelection(
//                Optional.absent(),
//                Optional.absent(),
//                Optional.absent(),
//                Optional.absent(),
//                Optional.of( edgeType1 ) );
//        Assert.assertEquals( 3, Iterables.size( lg.getEdges( edgeTypeOnly ) ) );
//    }
//
//    @Test
//    public void testDeleteVertex() {
//        LoomVertexKey v = createVertex();
//
//        lg.deleteVertex( v.getKey() );
//
//        Assert.assertNull( lg.getVertexById( v.getKey() ) );
//
//        //Check no edges to/from that vertex
//        EdgeSelection es1 = new EdgeSelection( Optional.of( v.getKey() ), Optional.absent(), Optional.absent(), Optional.absent(), Optional.absent() );
//        Assert.assertEquals( 0, Iterables.size( lg.getEdges( es1 ) ) );
//        EdgeSelection es2 = new EdgeSelection( Optional.absent(), Optional.absent(), Optional.of( v.getKey() ), Optional.absent(), Optional.absent() );
//        Assert.assertEquals( 0, Iterables.size( lg.getEdges( es2 ) ) );
//    }
//
//    @Test
//    public void testDeleteEdge() {
//        LoomVertexKey v1 = createVertex();
//        LoomVertexKey v2 = createVertex();
//        EntityKey label = TestDataFactory.entityKey();
//        EdgeKey edgeKey = new EdgeKey( v1.getKey(), v2.getKey(), label );
//
//        lg.addEdge( v1, v2, label );
//        LoomEdge edge = lg.getEdge( edgeKey );
//        Assert.assertNotNull( edge );
//
//        lg.deleteEdge( edgeKey );
//
//        Assert.assertNull( lg.getEdge( edgeKey ) );
//    }
//
//    @Test
//    public void testBulkCreateVertexAsync(){
//        List<LoomVertexFuture> futures = new ArrayList<>();
//
//        final int numTrials = 100;
//        EntityKey trackedKey = null;
//        for( int i = 0; i < numTrials; i++ ){
//            EntityKey key = TestDataFactory.entityKey();
//            if( i == numTrials/2 ){
//                trackedKey = key;
//            }
//            futures.add( lg.getOrCreateVertexAsync( key ) );
//        }
//
//        futures.forEach( LoomVertexFuture::get );
//
//        LoomVertexKey v = lg.getVertexByEntityKey( trackedKey );
//        Assert.assertNotNull( v );
//        Assert.assertEquals( trackedKey, v.getReference() );
//    }
//
//    @Test
//    public void testcreateEdgesAsync() {
//        LoomVertexKey vA = createVertex();
//
//        List<ResultSetFuture> futures = new ArrayList<>();
//
//        final int numTrials = 100;
//        for(int i = 0; i < numTrials; i++ ){
//            LoomVertexKey v = createVertex();
//            EntityKey label = TestDataFactory.entityKey();
//            futures.add( lg.addEdgeAsync( vA, v, label ) );
//        }
//
//        futures.forEach( ResultSetFuture::getUninterruptibly );
//
//        EdgeSelection fixSrc = new EdgeSelection(
//                Optional.of( vA.getKey() ),
//                Optional.absent(),
//                Optional.absent(),
//                Optional.absent(),
//                Optional.absent() );
//        Assert.assertEquals( numTrials, Iterables.size( lg.getEdges( fixSrc ) ) );
//    }
}
