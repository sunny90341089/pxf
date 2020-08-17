package org.greenplum.pxf.api.model;

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

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all plugin types (Accessor, Resolver, Fragmenter, ...).
 * Manages the meta data.
 */
public class BasePlugin implements Plugin {

    protected ConfigurationFactory configurationFactory;
    protected Logger LOG = LoggerFactory.getLogger(this.getClass());
    protected Configuration configuration;
    protected RequestContext context;
    private boolean initialized = false;

    public BasePlugin() {
        this(BaseConfigurationFactory.getInstance());
    }

    BasePlugin(ConfigurationFactory configurationFactory) {
        this.configurationFactory = configurationFactory;
    }

    /**
     * Initialize the plugin for the incoming request
     *
     * @param requestContext data provided in the request
     */
    @Override
    public void initialize(RequestContext requestContext) {
        this.context = requestContext;
        this.configuration = configurationFactory.
                initConfiguration(context.getConfig(), context.getServerName(), context.getUser(), context.getAdditionalConfigProps());
        this.initialized = true;
    }

    /**
     * Determines if the plugin is initialized
     *
     * @return true if initialized, false otherwise
     */
    public final boolean isInitialized() {
        return initialized;
    }
}
