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

package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.SerializedLambda;
import java.util.UUID;
import java.util.function.Function;

import org.objenesis.strategy.StdInstantiatorStrategy;
import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organization.Organization;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Preconditions;
import com.kryptnostic.conductor.rpc.ConductorCall;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;
import com.kryptnostic.conductor.rpc.Lambdas;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressWarnings( "rawtypes" )
@Component
public class ConductorCallStreamSerializer implements SelfRegisteringStreamSerializer<ConductorCall> {
    private static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {

        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();

            // https://github.com/EsotericSoftware/kryo/blob/master/test/com/esotericsoftware/kryo/serializers/Java8ClosureSerializerTest.java
            kryo.setInstantiatorStrategy(
                    new Kryo.DefaultInstantiatorStrategy(
                            new StdInstantiatorStrategy() ) );
            kryo.register( Object[].class );
            kryo.register( java.lang.Class.class );
            kryo.register( Organization.class );
            // Shared Lambdas
            kryo.register( Lambdas.class );
            kryo.register( SerializedLambda.class );

            // always needed for closure serialization, also if
            // registrationRequired=false
            kryo.register( ClosureSerializer.Closure.class,
                    new ClosureSerializer() );

            kryo.register( Function.class,
                    new ClosureSerializer() );

            return kryo;
        }
    };

    private ConductorSparkApi api;

    @Override
    @SuppressFBWarnings
    public void write( ObjectDataOutput out, ConductorCall object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getUserId() );
        Output output = new Output( (OutputStream) out );
        kryoThreadLocal.get().writeClassAndObject( output, object.getFunction() );
        output.flush();
    }

    @Override
    @SuppressWarnings( "unchecked" )
    @SuppressFBWarnings
    public ConductorCall read( ObjectDataInput in ) throws IOException {
        UUID userId = UUIDStreamSerializer.deserialize( in );
        Input input = new Input( (InputStream) in );
        Function<ConductorSparkApi, ?> f = (Function<ConductorSparkApi, ?>) kryoThreadLocal.get()
                .readClassAndObject( input );
        return new ConductorCall( userId, f, api );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.CONDUCTOR_CALL.ordinal();
    }

    @Override
    public void destroy() {

    }

    public synchronized void setConductorSparkApi( ConductorSparkApi api ) {
        Preconditions.checkState( this.api == null, "Api can only be set once" );
        this.api = Preconditions.checkNotNull( api );
    }

    @Override
    public Class<ConductorCall> getClazz() {
        return ConductorCall.class;
    }

}
