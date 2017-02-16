/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
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
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.linking;

import com.dataloom.graph.LinkingEdge;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class WeightedLinkingEdge implements Comparable<WeightedLinkingEdge> {
    private final Double      weight;
    private final LinkingEdge edge;

    public WeightedLinkingEdge( double weight, LinkingEdge edge ) {
        this.weight = weight;
        this.edge = edge;
    }

    public LinkingEdge getEdge() {
        return edge;
    }

    public double getWeight() {
        return weight;
    }

    @Override public int compareTo( WeightedLinkingEdge o ) {
        return weight.compareTo( o.weight );
    }
}
