/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.graph

import com.google.common.base.Preconditions.checkState
import org.apache.commons.lang.math.RandomUtils
import org.slf4j.LoggerFactory

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
private val logger = LoggerFactory.getLogger(ConnectedIntDiGraph::class.java)

data class ConnectedIntDiGraph internal constructor(private val adjMatrix: List<IntArray>) {
    constructor(graphQuery: SimpleGraphQuery) : this(fromEdges(graphQuery))

    init {
        checkState(isConnected(adjMatrix), "Graph must be connected.")
    }

    fun initialNeighborhood(initial: Int = RandomUtils.nextInt(adjMatrix.size)): GraphStep {
        val initialLayer = setOf(initial)
        val nextLayer = getNextLayerAndRemoveEdges(initialLayer)
        return GraphStep(nextLayer, initialLayer)
    }

    fun initialNeighborhood(vertices: Set<Int>): GraphStep {
        val nextLayer = getNextLayerAndRemoveEdges(vertices)
        return GraphStep(nextLayer, vertices)
    }

    fun nextNeighborhood(graphStep: GraphStep): GraphStep {
        val nextLayer = getNextLayerAndRemoveEdges(graphStep.neighborhood.keys)
        return GraphStep(nextLayer, graphStep.visited + nextLayer.keys)
    }

    fun hasRemaining(): Boolean {
        return adjMatrix.any { it.any { it >= 0 } }
    }

    private fun getNextLayerAndRemoveEdges(vertices: Set<Int>): Map<Int, Set<Int>> {
        val edgeList = getEdgeList(vertices)

        removeEdges(edgeList)

        return edgeList
                .groupBy({ it.second }, { it.first })
                .mapValues { it.value.toSet() }
    }

    private fun getEdgeList(vertices: Set<Int>): List<Pair<Int, Int>> {
        return vertices.flatMap { src -> adjMatrix[src].map { dst -> src to dst } }
    }

    private fun removeEdges(edgeList: List<Pair<Int, Int>>) {
        edgeList.forEach { adjMatrix[it.first][it.second] = -1 }
    }

    fun copyOf(): ConnectedIntDiGraph {
        return ConnectedIntDiGraph(adjMatrix.map { it.copyOf() })
    }
}

private fun fromEdges(graphQuery: SimpleGraphQuery): List<IntArray> {

    val n = graphQuery.entityConstraints.size
    val adjMatrix = arrayListOf<IntArray>()
    for (i in 0 until n) {
        adjMatrix[i] = IntArray(n) { -1 }
    }

    graphQuery.associationConstraints.forEachIndexed { index, ac ->
        adjMatrix[ac.association.first][ac.association.second] = index
    }

    return adjMatrix
}

private fun isConnected(adjMatrix: List<IntArray>): Boolean {
    val n = adjMatrix.size
    val indexRange = (0 until n)
    //O(2n) could be probably be better
    val connected = BooleanArray(n) { it == 0 || adjMatrix[it][0] >= -1 }
    var currentLayer = indexRange.filter { connected[it] }
    var count = currentLayer.size

    while (currentLayer.isNotEmpty()) {
        currentLayer = currentLayer
                .flatMap { src -> indexRange.filter { dst -> !connected[dst] && adjMatrix[src][dst] >= 0 } }
        count += currentLayer.size
    }

    return if (count == n) {
        true
    } else {
        logger.debug(
                "Submitted graph was not connected. The following constraints where not connected to 0-index constraint: {}",
                indexRange.filter { connected[it] })
        false
    }
}