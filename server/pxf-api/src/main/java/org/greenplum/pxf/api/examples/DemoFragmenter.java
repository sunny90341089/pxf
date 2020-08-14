package org.greenplum.pxf.api.examples;

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

import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Class that defines the splitting of a data resource into fragments that can be processed in parallel
 * getFragments() returns the fragments information of a given path (source name and location of each fragment).
 * <p>
 * Demo implementation
 */
@Component("DemoFragmenter")
public class DemoFragmenter extends BaseFragmenter {

    /**
     * Provide metadata for each data partition of the given datasource
     *
     * @return list of fragments
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        List<Fragment> fragments = new ArrayList<>(3);
        String localhostname = java.net.InetAddress.getLocalHost().getHostName();
        String[] localHosts = new String[]{localhostname, localhostname};
        fragments.add(new Fragment(context.getDataSource() + ".1", localHosts, new DemoFragmentMetadata("fragment1")));
        fragments.add(new Fragment(context.getDataSource() + ".2", localHosts, new DemoFragmentMetadata("fragment2")));
        fragments.add(new Fragment(context.getDataSource() + ".3", localHosts, new DemoFragmentMetadata("fragment3")));
        return fragments;
    }
}
