package com.dataloom.graph;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.dataloom.authorization.HzAuthzTest;
import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.GraphQueryService;
import com.dataloom.graph.core.LoomGraph;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.EdgeSelection;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.graph.core.objects.LoomVertex;
import com.dataloom.graph.core.objects.LoomVertexFuture;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;

public class LoomGraphTest extends HzAuthzTest {

    protected static final LoomGraph         lg;
    protected static final GraphQueryService gqs;

    static {
        gqs = new GraphQueryService( session );
        lg = new LoomGraph( gqs );
        LoomVertexFuture.setGraphQueryService( gqs );
    }

    private LoomVertex createVertex() {
        return getOrCreateVertex( TestDataFactory.entityKey() );
    }

    private LoomVertex getOrCreateVertex( EntityKey entityKey ) {
        LoomVertex v = lg.getOrCreateVertex( entityKey );
        // verification
        Assert.assertNotNull( v );
        Assert.assertEquals( v, lg.getVertexByEntityKey( entityKey ) );
        Assert.assertEquals( v, lg.getVertexById( v.getKey() ) );

        return v;
    }

    @Test
    public void testCreateVertex() {
        createVertex();
    }

    @Test
    public void testCreateVertexAsync() {
        EntityKey key1 = TestDataFactory.entityKey();
        EntityKey key2 = TestDataFactory.entityKey();

        LoomVertexFuture v1Async = lg.getOrCreateVertexAsync( key1 );
        LoomVertexFuture v2Async = lg.getOrCreateVertexAsync( key2 );

        LoomVertex v1 = v1Async.get();
        LoomVertex v2 = v2Async.get();

        // verification
        Assert.assertEquals( v1, lg.getVertexByEntityKey( key1 ) );
        Assert.assertEquals( v1, lg.getVertexById( v1.getKey() ) );
        Assert.assertEquals( v2, lg.getVertexByEntityKey( key2 ) );
        Assert.assertEquals( v2, lg.getVertexById( v2.getKey() ) );
    }

    @Test
    public void testCreateEdge() {
        LoomVertex v1 = createVertex();
        LoomVertex v2 = createVertex();
        EntityKey label = TestDataFactory.entityKey();

        lg.addEdge( v1, v2, label );
        LoomEdge edge = lg.getEdge( new EdgeKey( v1.getKey(), v2.getKey(), label ) );

        // verification
        Assert.assertNotNull( edge );
        Assert.assertEquals( v1.getKey(), edge.getKey().getSrcId() );
        Assert.assertEquals( v1.getReference().getEntitySetId(), edge.getSrcType() );
        Assert.assertEquals( v2.getKey(), edge.getKey().getDstId() );
        Assert.assertEquals( v2.getReference().getEntitySetId(), edge.getDstType() );
        Assert.assertEquals( label, edge.getReference() );
    }

    @Test
    public void testGetEdges() {
        LoomVertex vA = createVertex();
        LoomVertex vB = createVertex();
        LoomVertex vC = createVertex();

        UUID edgeType1 = UUID.randomUUID();
        UUID edgeType2;
        do {
            edgeType2 = UUID.randomUUID();
        } while ( edgeType2 == edgeType1 );

        EntityKey labelAB = TestDataFactory.entityKey( edgeType1 );
        EntityKey labelBC = TestDataFactory.entityKey( edgeType1 );
        EntityKey labelAC = TestDataFactory.entityKey( edgeType1 );
        EntityKey labelCA = TestDataFactory.entityKey( edgeType2 );

        lg.addEdge( vA, vB, labelAB );
        lg.addEdge( vB, vC, labelBC );
        lg.addEdge( vA, vC, labelAC );
        lg.addEdge( vC, vA, labelCA );

        EdgeSelection srcIdOnly = new EdgeSelection(
                Optional.of( vA.getKey() ),
                Optional.absent(),
                Optional.absent(),
                Optional.absent(),
                Optional.absent() );
        Assert.assertEquals( Iterables.size( lg.getEdges( srcIdOnly ) ), 2 );

        EdgeSelection srcTypeOnly = new EdgeSelection(
                Optional.absent(),
                Optional.of( vA.getReference().getEntitySetId() ),
                Optional.absent(),
                Optional.absent(),
                Optional.absent() );
        Assert.assertEquals( Iterables.size( lg.getEdges( srcTypeOnly ) ), 2 );

        EdgeSelection dstIdOnly = new EdgeSelection(
                Optional.absent(),
                Optional.absent(),
                Optional.of( vB.getKey() ),
                Optional.absent(),
                Optional.absent() );
        Assert.assertEquals( Iterables.size( lg.getEdges( dstIdOnly ) ), 1 );

        EdgeSelection dstTypeOnly = new EdgeSelection(
                Optional.absent(),
                Optional.absent(),
                Optional.absent(),
                Optional.of( vB.getReference().getEntitySetId() ),
                Optional.absent() );
        Assert.assertEquals( Iterables.size( lg.getEdges( dstTypeOnly ) ), 2 );

        EdgeSelection edgeTypeOnly = new EdgeSelection(
                Optional.absent(),
                Optional.absent(),
                Optional.absent(),
                Optional.absent(),
                Optional.of( edgeType1 ) );
        Assert.assertEquals( Iterables.size( lg.getEdges( edgeTypeOnly ) ), 3 );
    }

    // @Test
    public void testDeleteVertex() {
        LoomVertex v = createVertex();

        lg.deleteVertex( v.getKey() );

        Assert.assertNull( lg.getVertexById( v.getKey() ) );
    }

    @Test
    public void testDeleteEdge() {
        LoomVertex v1 = createVertex();
        LoomVertex v2 = createVertex();
        EntityKey label = TestDataFactory.entityKey();
        EdgeKey edgeKey = new EdgeKey( v1.getKey(), v2.getKey(), label );

        lg.addEdge( v1, v2, label );
        LoomEdge edge = lg.getEdge( edgeKey );
        Assert.assertNotNull( edge );

        lg.deleteEdge( edgeKey );

        Assert.assertNull( lg.getEdge( edgeKey ) );
    }

    // TODO createVertexAsync, createEdgeAsync, deleteEdges
}
