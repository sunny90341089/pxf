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


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.plugins.hdfs.utilities.PxfInputFormat;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Fragmenter class for HDFS data resources.
 * <p>
 * Given an HDFS data source (a file, directory, or wild card pattern) divide
 * the data into fragments and return a list of them along with a list of
 * host:port locations for each.
 */
@Component("HdfsDataFragmenter")
@RequestScope
public class HdfsDataFragmenter extends BaseFragmenter {

    protected static final String IGNORE_MISSING_PATH_OPTION = "IGNORE_MISSING_PATH";

    protected JobConf jobConf;
    protected HcfsType hcfsType;

    @Override
    public void afterPropertiesSet() {
        // Check if the underlying configuration is for HDFS
        hcfsType = HcfsType.getHcfsType(context);
        jobConf = new JobConf(configuration, this.getClass());
    }

    /**
     * Gets the fragments for a data source URI that can appear as a file name,
     * a directory name or a wildcard. Returns the data fragments in JSON
     * format.
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        // TODO: do we need to revert to hcfsType.getDataUri(jobConf, context)
        Path path = new Path(hcfsType.getDataUri(context));
        List<InputSplit> splits;
        try {
            splits = getSplits(path);
        } catch (InvalidInputException e) {
            if (StringUtils.equalsIgnoreCase("true", context.getOption(IGNORE_MISSING_PATH_OPTION))) {
                LOG.debug("Ignoring InvalidInputException", e);
                return fragments;
            }
            throw e;
        }

        LOG.debug("Total number of fragments = {}", splits.size());
        for (InputSplit split : splits) {
            FileSplit fsp = (FileSplit) split;
            String filepath = fsp.getPath().toString();
            String[] hosts = fsp.getLocations();

            /*
             * metadata information includes: file split's start, length and
             * hosts (locations).
             */

            Fragment fragment = new Fragment(filepath, hosts, new HcfsFragmentMetadata(fsp));
            fragments.add(fragment);
        }

        return fragments;
    }

    @Override
    public FragmentStats getFragmentStats() throws Exception {
        // TODO: make sure jobConf is not necessary or revert to hcfsType.getDataUri(jobConf, context)
        String absoluteDataPath = hcfsType.getDataUri(context);
        List<InputSplit> splits = getSplits(new Path(absoluteDataPath));

        if (splits.isEmpty()) {
            return new FragmentStats(0, 0, 0);
        }
        long totalSize = 0;
        for (InputSplit split : splits) {
            totalSize += split.getLength();
        }
        InputSplit firstSplit = splits.get(0);
        return new FragmentStats(splits.size(), firstSplit.getLength(), totalSize);
    }

    protected List<InputSplit> getSplits(Path path) throws IOException {
        PxfInputFormat pxfInputFormat = new PxfInputFormat();
        PxfInputFormat.setInputPaths(jobConf, path);
        InputSplit[] splits = pxfInputFormat.getSplits(jobConf, 1);
        List<InputSplit> result = new ArrayList<>();

        /*
         * HD-2547: If the file is empty, an empty split is returned: no
         * locations and no length.
         */
        if (splits != null) {
            for (InputSplit split : splits) {
                if (split.getLength() > 0) {
                    result.add(split);
                }
            }
        }

        return result;
    }
}
