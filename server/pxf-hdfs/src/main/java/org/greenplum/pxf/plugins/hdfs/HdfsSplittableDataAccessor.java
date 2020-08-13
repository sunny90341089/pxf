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


import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.IOException;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 * Accessor for accessing a splittable HDFS data sources. HDFS will divide the
 * file into splits based on an internal decision (by default, the block size is
 * also the split size).
 * <p>
 * Accessors that require such base functionality should extend this class.
 */
public abstract class HdfsSplittableDataAccessor extends HcfsBaseAccessor {
    protected RecordReader<Object, Object> reader;
    protected InputFormat<?, ?> inputFormat;
    protected JobConf jobConf;
    protected Object key, data;
    protected FileSplit fileSplit;
    HcfsType hcfsType;

    /**
     * Constructs an HdfsSplittableDataAccessor
     *
     * @param inFormat the HDFS {@link InputFormat} the caller wants to use
     */
    protected HdfsSplittableDataAccessor(InputFormat<?, ?> inFormat) {
        inputFormat = inFormat;
    }

    @Override
    public void afterPropertiesSet() {
        // variable required for the splits iteration logic
        jobConf = new JobConf(configuration, HdfsSplittableDataAccessor.class);

        // Check if the underlying configuration is for HDFS
        hcfsType = HcfsType.getHcfsType(context);

        // Parse fileSplit from context
        fileSplit = HdfsUtilities.parseFileSplit(context);
    }

    /**
     * Fetches the requested fragment (file split) for the current client
     * request, and sets a record reader for the job.
     *
     * @return true if succeeded, false if no more splits to be read
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean openForRead() throws Exception {
        reader = (RecordReader<Object, Object>) getReader(jobConf, fileSplit);
        key = reader.createKey();
        data = reader.createValue();
        return true;
    }

    /**
     * Specialized accessors will override this method and implement their own
     * recordReader. For example, a plain delimited text accessor may want to
     * return a LineRecordReader.
     *
     * @param jobConf the hadoop jobconf to use for the selected InputFormat
     * @param split   the input split to be read by the accessor
     * @return a recordreader to be used for reading the data records of the
     * split
     * @throws IOException if recordreader could not be created
     */
    abstract protected Object getReader(JobConf jobConf, InputSplit split)
            throws IOException;

    /**
     * Fetches one record from the file. The record is returned as a Java
     * object.
     */
    @Override
    public OneRow readNextObject() throws IOException {
        // if there is one more record in the current split
        if (!reader.next(key, data)) {
            return null;
        }

        /*
         * if neither condition was met, it means we already read all the
         * records in all the splits, and in this call record variable was not
         * set, so we return null and thus we are signaling end of records
         * sequence
         */
        return new OneRow(key, data);
    }

    /**
     * When user finished reading the file, it closes the RecordReader
     */
    @Override
    public void closeForRead() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }

}
