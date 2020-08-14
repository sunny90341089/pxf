package org.greenplum.pxf.service;

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


import org.apache.commons.io.IOUtils;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BridgeInputBuilderTest {
    RequestContext mockContext;
    BridgeInputBuilder inputBuilder;
    DataInputStream inputStream;

    @Test
    /*
     * Test makeInput method: small \n terminated input
     */
    public void makeInput() throws Exception {

        byte[] data = new byte[]{
                (int) 'a',
                (int) 'b',
                (int) 'c',
                (int) 'd',
                (int) '\n',
                (int) 'n',
                (int) 'o',
                (int) '\n'};

        prepareInput(data);

        List<OneField> record = inputBuilder.makeInput(inputStream);

        // the inputStream is exhausted completely, so we check line breaks too
        verifyRecord(record, Arrays.copyOfRange(data, 0, 8));
    }

    @Test
    /*
     * Test the makeInput method: input > buffer size, \n terminated
     */
    public void makeInputBigArray() throws Exception {

        byte[] bigArray = new byte[2000];
        for (int i = 0; i < 1999; ++i) {
            bigArray[i] = (byte) (i % 10 + 30);
        }
        bigArray[1999] = (byte) '\n';

        prepareInput(bigArray);

        List<OneField> record = inputBuilder.makeInput(inputStream);

        verifyRecord(record, bigArray);
    }

    @Test
    /*
     * Test the makeInput method: input > buffer size, no \n
     */
    public void makeInputBigArrayNoNewLine() throws Exception {

        byte[] bigArray = new byte[2000];
        for (int i = 0; i < 2000; ++i) {
            bigArray[i] = (byte) (i % 10 + 60);
        }

        prepareInput(bigArray);

        List<OneField> record = inputBuilder.makeInput(inputStream);

        verifyRecord(record, bigArray);
    }

    @Test
    /*
     * Test the makeInput method: empty stream (returns -1)
     */
    public void makeInputEmptyStream() throws Exception {

        byte[] empty = new byte[0];

        prepareInput(empty);

        List<OneField> record = inputBuilder.makeInput(inputStream);

        verifyRecord(record, empty);
    }

    /*
     * helpers functions
     */

    @AfterEach
    public void cleanUp() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }

    private void prepareInput(byte[] data) {
        mockContext = mock(RequestContext.class);
        when(mockContext.getOutputFormat()).thenReturn(OutputFormat.TEXT);
        inputBuilder = new BridgeInputBuilder(mockContext);
        inputStream = new DataInputStream(
                new ByteArrayInputStream(data));
    }

    private void verifyRecord(List<OneField> record, byte[] expected) throws IOException {
        assertEquals(record.size(), 1);

        OneField field = record.get(0);
        assertEquals(field.type, DataType.BYTEA.getOID());
        assertTrue(field.val instanceof InputStream);

        byte[] bytes = IOUtils.toByteArray((InputStream) field.val);
        byte[] result = Arrays.copyOfRange(bytes, 0, bytes.length);
        assertEquals(result.length, expected.length);
        assertArrayEquals(result, expected);
    }
}
