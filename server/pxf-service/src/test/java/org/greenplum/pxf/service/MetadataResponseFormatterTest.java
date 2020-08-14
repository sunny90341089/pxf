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

import org.greenplum.pxf.api.model.Metadata;
import org.greenplum.pxf.api.utilities.EnumGpdbType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MetadataResponseFormatterTest {

    MetadataResponse response = null;

    private String convertResponseToString(MetadataResponse data) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        data.writeTo(outputStream);
        return outputStream.toString();
    }

    @Test
    public void formatResponseString() throws Exception {
        List<Metadata> metadataList = new ArrayList<>();
        List<Metadata.Field> fields = new ArrayList<>();
        Metadata.Item itemName = new Metadata.Item("default", "table1");
        Metadata metadata = new Metadata(itemName, fields);
        fields.add(new Metadata.Field("field1", EnumGpdbType.Int8Type, "bigint"));
        fields.add(new Metadata.Field("field2", EnumGpdbType.TextType, "string"));
        metadataList.add(metadata);

        response = MetadataResponseFormatter.formatResponse(metadataList, "path.file");

        String expected = "{\"PXFMetadata\":[{" + "\"item\":{\"path\":\"default\",\"name\":\"table1\"}," +
                "\"fields\":[{\"name\":\"field1\",\"type\":\"int8\",\"sourceType\":\"bigint\",\"complexType\":false},{\"name\":\"field2\",\"type\":\"text\",\"sourceType\":\"string\",\"complexType\":false}]}]}";
        assertEquals(expected, convertResponseToString(response));
    }

    @Test
    public void formatResponseStringWithNullModifier() throws Exception {
        List<Metadata> metadataList = new ArrayList<>();
        List<Metadata.Field> fields = new ArrayList<>();
        Metadata.Item itemName = new Metadata.Item("default", "table1");
        Metadata metadata = new Metadata(itemName, fields);
        fields.add(new Metadata.Field("field1", EnumGpdbType.Int8Type, "bigint", null));
        fields.add(new Metadata.Field("field2", EnumGpdbType.TextType, "string", new String[]{}));
        metadataList.add(metadata);

        response = MetadataResponseFormatter.formatResponse(metadataList, "path.file");

        String expected = "{\"PXFMetadata\":[{" + "\"item\":{\"path\":\"default\",\"name\":\"table1\"}," +
                "\"fields\":[{\"name\":\"field1\",\"type\":\"int8\",\"sourceType\":\"bigint\",\"complexType\":false},{\"name\":\"field2\",\"type\":\"text\",\"sourceType\":\"string\",\"complexType\":false}]}]}";
        assertEquals(expected, convertResponseToString(response));
    }

    @Test
    public void formatResponseStringWithModifiers() throws Exception {
        List<Metadata> metadataList = new ArrayList<>();
        List<Metadata.Field> fields = new ArrayList<>();
        Metadata.Item itemName = new Metadata.Item("default", "table1");
        Metadata metadata = new Metadata(itemName, fields);
        fields.add(new Metadata.Field("field1", EnumGpdbType.Int8Type, "bigint"));
        fields.add(new Metadata.Field("field2", EnumGpdbType.NumericType, "decimal",
                new String[]{"1349", "1789"}));
        fields.add(new Metadata.Field("field3", EnumGpdbType.BpcharType, "char",
                new String[]{"50"}));
        metadataList.add(metadata);

        response = MetadataResponseFormatter.formatResponse(metadataList, "path.file");

        String expected = "{\"PXFMetadata\":[{" + "\"item\":{\"path\":\"default\",\"name\":\"table1\"}," +
                "\"fields\":[" +
                "{\"name\":\"field1\",\"type\":\"int8\",\"sourceType\":\"bigint\",\"complexType\":false}," +
                "{\"name\":\"field2\",\"type\":\"numeric\",\"sourceType\":\"decimal\",\"modifiers\":[\"1349\",\"1789\"],\"complexType\":false}," +
                "{\"name\":\"field3\",\"type\":\"bpchar\",\"sourceType\":\"char\",\"modifiers\":[\"50\"],\"complexType\":false}" +
                "]}]}";
        assertEquals(expected, convertResponseToString(response));
    }

    @Test
    public void formatResponseStringWithSourceType() throws Exception {
        List<Metadata> metadataList = new ArrayList<>();
        List<Metadata.Field> fields = new ArrayList<>();
        Metadata.Item itemName = new Metadata.Item("default", "table1");
        Metadata metadata = new Metadata(itemName, fields);
        fields.add(new Metadata.Field("field1", EnumGpdbType.Float8Type, "double"));
        metadataList.add(metadata);

        response = MetadataResponseFormatter.formatResponse(metadataList, "path.file");

        String expected = "{\"PXFMetadata\":[{" + "\"item\":{\"path\":\"default\",\"name\":\"table1\"}," +
                "\"fields\":[" +
                "{\"name\":\"field1\",\"type\":\"float8\",\"sourceType\":\"double\",\"complexType\":false}" +
                "]}]}";
        assertEquals(expected, convertResponseToString(response));
    }

    @Test
    public void formatResponseStringNull() throws Exception {
        response = MetadataResponseFormatter.formatResponse(null, "path.file");
        String expected = "{\"PXFMetadata\":[]}";

        assertEquals(expected, convertResponseToString(response));
    }

    @Test
    public void formatResponseStringNoFields() throws Exception {
        List<Metadata> metadataList = new ArrayList<>();
        Metadata.Item itemName = new Metadata.Item("default", "table1");
        Metadata metadata = new Metadata(itemName, null);
        metadataList.add(metadata);
        response = MetadataResponseFormatter.formatResponse(metadataList, "path.file");
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> convertResponseToString(response),
                "formatting should fail because fields field is null");
        assertEquals("metadata for " + metadata.getItem() + " contains no fields - cannot serialize", e.getMessage());

        ArrayList<Metadata.Field> fields = new ArrayList<>();
        metadataList = new ArrayList<>();
        metadata = new Metadata(itemName, fields);
        metadataList.add(metadata);
        response = MetadataResponseFormatter.formatResponse(metadataList, "path.file");
        e = assertThrows(IllegalArgumentException.class,
                () -> convertResponseToString(response),
                "formatting should fail because there are no fields");
        assertEquals("metadata for " + metadata.getItem() + " contains no fields - cannot serialize", e.getMessage());
    }

    @Test
    public void formatResponseStringPartialNull() throws Exception {
        List<Metadata> metadataList = new ArrayList<>();
        List<Metadata.Field> fields = new ArrayList<>();
        Metadata.Item itemName = new Metadata.Item("default", "table1");
        Metadata metadata = new Metadata(itemName, fields);
        fields.add(new Metadata.Field("field1", EnumGpdbType.Int8Type, "bigint"));
        metadataList.add(null);
        metadataList.add(metadata);
        response = MetadataResponseFormatter.formatResponse(metadataList, "path.file");
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> convertResponseToString(response),
                "formatting should fail because one of the metdata object is null");
        assertEquals("metadata object is null - cannot serialize", e.getMessage());
    }

    @Test
    public void formatResponseStringWithMultipleItems() throws Exception {
        List<Metadata> metdataList = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            List<Metadata.Field> fields = new ArrayList<>();
            Metadata.Item itemName = new Metadata.Item("default", "table" + i);
            Metadata metadata = new Metadata(itemName, fields);
            fields.add(new Metadata.Field("field1", EnumGpdbType.Int8Type, "bigint"));
            fields.add(new Metadata.Field("field2", EnumGpdbType.TextType, "string"));
            metdataList.add(metadata);
        }
        response = MetadataResponseFormatter.formatResponse(metdataList, "path.file");

        StringBuilder expected = new StringBuilder();
        for (int i = 1; i <= 10; i++) {
            if (i == 1) {
                expected.append("{\"PXFMetadata\":[");
            } else {
                expected.append(",");
            }
            expected.append("{\"item\":{\"path\":\"default\",\"name\":\"table").append(i).append("\"},");
            expected.append("\"fields\":[{\"name\":\"field1\",\"type\":\"int8\",\"sourceType\":\"bigint\",\"complexType\":false},{\"name\":\"field2\",\"type\":\"text\",\"sourceType\":\"string\",\"complexType\":false}]}");
        }
        expected.append("]}");

        assertEquals(expected.toString(), convertResponseToString(response));
    }

    @Test
    public void formatResponseStringWithMultiplePathsAndItems() throws Exception {
        List<Metadata> metdataList = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            List<Metadata.Field> fields = new ArrayList<>();
            Metadata.Item itemName = new Metadata.Item("default" + i, "table" + i);
            Metadata metadata = new Metadata(itemName, fields);
            fields.add(new Metadata.Field("field1", EnumGpdbType.Int8Type, "bigint"));
            fields.add(new Metadata.Field("field2", EnumGpdbType.TextType, "string"));
            metdataList.add(metadata);
        }
        response = MetadataResponseFormatter.formatResponse(metdataList, "path.file");
        StringBuilder expected = new StringBuilder();
        for (int i = 1; i <= 10; i++) {
            if (i == 1) {
                expected.append("{\"PXFMetadata\":[");
            } else {
                expected.append(",");
            }
            expected.append("{\"item\":{\"path\":\"default").append(i).append("\",\"name\":\"table").append(i).append("\"},");
            expected.append("\"fields\":[{\"name\":\"field1\",\"type\":\"int8\",\"sourceType\":\"bigint\",\"complexType\":false},{\"name\":\"field2\",\"type\":\"text\",\"sourceType\":\"string\",\"complexType\":false}]}");
        }
        expected.append("]}");

        assertEquals(expected.toString(), convertResponseToString(response));
    }
}

