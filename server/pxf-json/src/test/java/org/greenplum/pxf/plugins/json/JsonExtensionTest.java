package org.greenplum.pxf.plugins.json;

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

import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.error.BadRecordException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonExtensionTest extends PxfUnit {

    private static final String IDENTIFIER = JsonAccessor.IDENTIFIER_PARAM;
    private List<Pair<String, DataType>> columnDefs = null;
    private final List<Pair<String, String>> extraParams = new ArrayList<>();
    private final List<String> output = new ArrayList<>();

    @BeforeEach
    public void before() {

        columnDefs = new ArrayList<>();

        columnDefs.add(new Pair<>("created_at", DataType.TEXT));
        columnDefs.add(new Pair<>("id", DataType.BIGINT));
        columnDefs.add(new Pair<>("text", DataType.TEXT));
        columnDefs.add(new Pair<>("user.screen_name", DataType.TEXT));
        columnDefs.add(new Pair<>("entities.hashtags[0]", DataType.TEXT));
        columnDefs.add(new Pair<>("coordinates.coordinates[0]", DataType.FLOAT8));
        columnDefs.add(new Pair<>("coordinates.coordinates[1]", DataType.FLOAT8));

        output.clear();
        extraParams.clear();
    }

    @AfterEach
    public void cleanup() {
        columnDefs.clear();
    }

    @Test
    public void testCompressedMultilineJsonFile() throws Exception {

        extraParams.add(new Pair<>(IDENTIFIER, "created_at"));

        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

        super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/tweets.tar.gz"), output);
    }

    @Test
    public void testMaxRecordLength() throws Exception {

        // variable-size-objects.json contains 3 json objects but only 2 of them fit in the 27 byte length limitation

        extraParams.add(new Pair<>(IDENTIFIER, "key666"));
        extraParams.add(new Pair<>("MAXLENGTH", "27"));

        columnDefs.clear();
        columnDefs.add(new Pair<>("key666", DataType.TEXT));

        output.add("small object1");
        // skip the large object2 XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
        output.add("small object3");

        super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/variable-size-objects.json"), output);
    }

    @Test
    public void testDataTypes() throws Exception {

        // TODO: The BYTEA type is not tested. The implementation (val.asText().getBytes()) returns an array reference
        // and it is not clear whether this is the desired behavior.
        //
        // For the time being avoid using BYTEA type!!!

        // This test also verifies that the order of the columns in the table definition agnostic to the order of the
        // json attributes.

        extraParams.add(new Pair<>(IDENTIFIER, "bintType"));

        columnDefs.clear();

        columnDefs.add(new Pair<>("text", DataType.TEXT));
        columnDefs.add(new Pair<>("varcharType", DataType.VARCHAR));
        columnDefs.add(new Pair<>("bpcharType", DataType.BPCHAR));
        columnDefs.add(new Pair<>("smallintType", DataType.SMALLINT));
        columnDefs.add(new Pair<>("integerType", DataType.INTEGER));
        columnDefs.add(new Pair<>("realType", DataType.REAL));
        columnDefs.add(new Pair<>("float8Type", DataType.FLOAT8));
        // The DataType.BYTEA type is left out for further validation.
        columnDefs.add(new Pair<>("booleanType", DataType.BOOLEAN));
        columnDefs.add(new Pair<>("bintType", DataType.BIGINT));

        output.add(",varcharType,bpcharType,777,999,3.15,3.14,true,666");

        super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/datatypes-test.json"), output);
    }

    @Test
    public void testMissingArrayJsonAttribute() {

        extraParams.add(new Pair<>(IDENTIFIER, "created_at"));

        columnDefs.clear();

        columnDefs.add(new Pair<>("created_at", DataType.TEXT));
        // User is not an array! An attempt to access it should throw an exception!
        columnDefs.add(new Pair<>("user[0]", DataType.TEXT));

        assertThrows(IllegalStateException.class,
                () -> super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
                        + "src/test/resources/tweets-with-missing-text-attribtute.json"), output));
    }

    @Test
    public void testMissingJsonAttribute() throws Exception {

        extraParams.add(new Pair<>(IDENTIFIER, "created_at"));

        // Missing attributes are substituted by an empty field
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,,SpreadButter,tweetCongress,,");

        super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/tweets-with-missing-text-attribtute.json"), output);
    }

    @Test
    public void testMalformedJsonObject() {

        extraParams.add(new Pair<>(IDENTIFIER, "created_at"));

        BadRecordException e = assertThrows(BadRecordException.class,
                () -> super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
                        + "src/test/resources/tweets-broken.json"), output));
        assertTrue(e.getMessage().contains("error while parsing json record 'Unexpected character (':' (code 58)): was expecting comma to separate"));
    }

    @Test
    public void testMismatchedTypes() {

        BadRecordException e = assertThrows(BadRecordException.class,
                () -> super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
                        + "src/test/resources/mismatched-types.json"), output));
        assertEquals("invalid BIGINT input value '\"[\"'", e.getMessage());
    }

    @Test
    public void testSmallTweets() throws Exception {

        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");
        output.add("Fri Jun 07 22:45:03 +0000 2013,343136551322136576,text4,SevenStonesBuoy,,-6.1,50.103");

        super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/tweets-small.json"), output);
    }

    @Test
    public void testTweetsWithNull() throws Exception {

        output.add("Fri Jun 07 22:45:02 +0000 2013,,text1,SpreadButter,tweetCongress,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,,text2,patronusdeadly,,,");

        super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/null-tweets.json"), output);
    }

    @Test
    public void testSmallTweetsWithDelete() throws Exception {

        output.add(",,,,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

        super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/tweets-small-with-delete.json"), output);
    }

    @Test
    public void testWellFormedJson() throws Exception {

        extraParams.add(new Pair<>(IDENTIFIER, "created_at"));

        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

        super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/tweets-pp.json"), output);
    }

    @Test
    public void testWellFormedJsonWithDelete() throws Exception {

        extraParams.add(new Pair<>(IDENTIFIER, "created_at"));

        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

        super.assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/tweets-pp-with-delete.json"), output);
    }

    @Test
    public void testMultipleFiles() throws Exception {

        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");
        output.add("Fri Jun 07 22:45:03 +0000 2013,343136551322136576,text4,SevenStonesBuoy,,-6.1,50.103");
        output.add(",,,,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

        super.assertUnorderedOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/tweets-small*.json"), output);
    }

    @Override
    public List<Pair<String, String>> getExtraParams() {
        return extraParams;
    }

    @Override
    public Class<? extends Fragmenter> getFragmenterClass() {
        return HdfsDataFragmenter.class;
    }

    @Override
    public Class<? extends Accessor> getAccessorClass() {
        return JsonAccessor.class;
    }

    @Override
    public Class<? extends Resolver> getResolverClass() {
        return JsonResolver.class;
    }

    @Override
    public List<Pair<String, DataType>> getColumnDefinitions() {
        return columnDefs;
    }
}
