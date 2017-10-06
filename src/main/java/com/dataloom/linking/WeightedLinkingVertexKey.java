package com.dataloom.linking;

public class WeightedLinkingVertexKey implements Comparable<WeightedLinkingVertexKey> {
    double           weight;
    LinkingVertexKey vertexKey;

    public WeightedLinkingVertexKey( double weight, LinkingVertexKey vertexKey ) {
        this.weight = weight;
        this.vertexKey = vertexKey;
    }

    public double getWeight() {
        return weight;
    }

    public LinkingVertexKey getVertexKey() {
        return vertexKey;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        WeightedLinkingVertexKey that = (WeightedLinkingVertexKey) o;

        if ( Double.compare( that.weight, weight ) != 0 )
            return false;
        return vertexKey.equals( that.vertexKey );
    }

    @Override public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits( weight );
        result = (int) ( temp ^ ( temp >>> 32 ) );
        result = 31 * result + vertexKey.hashCode();
        return result;
    }

    @Override public String toString() {
        return "WeightedLinkingVertexKey{" +
                "weight=" + weight +
                ", vertexKey=" + vertexKey +
                '}';
    }

    @Override public int compareTo( WeightedLinkingVertexKey o ) {
        return Double.compare( weight, o.weight );
    }
}
