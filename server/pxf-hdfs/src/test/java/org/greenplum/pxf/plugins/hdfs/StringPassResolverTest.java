package org.greenplum.pxf.plugins.hdfs;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class StringPassResolverTest {
    RequestContext context;

    /*
     * Test the setFields method: small input
     */
    @Test
    public void testSetFields() {
        StringPassResolver resolver = buildResolver();

        byte[] data = new byte[]{
                (int) 'a',
                (int) 'b',
                (int) 'c',
                (int) 'd',
                (int) '\n',
                (int) 'n',
                (int) 'o',
                (int) '\n'};

        List<OneField> record = Collections.singletonList(new OneField(DataType.BYTEA.getOID(),
                Arrays.copyOfRange(data, 0, 5)));

        OneRow oneRow = resolver.setFields(record);
        verifyOneRow(oneRow, Arrays.copyOfRange(data, 0, 5));

        record = Collections.singletonList(new OneField(DataType.BYTEA.getOID(),
                Arrays.copyOfRange(data, 5, 8)));

        oneRow = resolver.setFields(record);
        verifyOneRow(oneRow, Arrays.copyOfRange(data, 5, 8));
    }

    @Test
    /*
     * Test the setFields method: empty byte array
     */
    public void testSetFieldsEmptyByteArray() {

        StringPassResolver resolver = buildResolver();

        byte[] empty = new byte[0];

        List<OneField> record = Collections.singletonList(new OneField(DataType.BYTEA.getOID(),
                empty));

        OneRow oneRow = resolver.setFields(record);

        assertNull(oneRow);
    }

    /*
     * helpers functions
     */
    private StringPassResolver buildResolver() {
        context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        StringPassResolver resolver = new StringPassResolver();
        resolver.setRequestContext(context);
        return resolver;
    }

    private void verifyOneRow(OneRow oneRow, byte[] expected) {
        assertNull(oneRow.getKey());
        byte[] bytes = (byte[]) oneRow.getData();
        byte[] result = Arrays.copyOfRange(bytes, 0, bytes.length);
        assertEquals(result.length, expected.length);
        assertArrayEquals(result, expected);
    }
}
