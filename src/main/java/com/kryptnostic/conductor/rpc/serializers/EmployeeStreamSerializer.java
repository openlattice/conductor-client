package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.conductor.rpc.Employee;
import com.kryptnostic.mapstores.v1.constants.HazelcastSerializerTypeIds;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class EmployeeStreamSerializer implements SelfRegisteringStreamSerializer<Employee> {

    @Override
    public void write( ObjectDataOutput out, Employee object ) throws IOException {
        out.writeUTF( object.getName() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDept() );
        out.writeInt( object.getSalary() );

    }

    @Override
    public Employee read( ObjectDataInput in ) throws IOException {
        return new Employee(
                in.readUTF(),
                in.readUTF(),
                in.readUTF(),
                in.readInt() );
    }

    @Override
    public int getTypeId() {

        System.out.println("It's Employee!!");
        return HazelcastSerializerTypeIds.EMPLOYEE.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<Employee> getClazz() {
        return Employee.class;
    }

}
