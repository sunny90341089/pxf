package org.greenplum.pxf.api.serializer.binary;

import java.io.DataOutputStream;
import java.io.IOException;

public class IntegerValueHandler<T extends Number> extends BaseBinaryValueHandler<T> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final T value) throws IOException {
        buffer.writeInt(4);
        buffer.writeInt(value.intValue());
    }
}