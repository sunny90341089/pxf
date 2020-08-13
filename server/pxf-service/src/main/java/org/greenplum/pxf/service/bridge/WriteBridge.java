package org.greenplum.pxf.service.bridge;

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

import org.greenplum.pxf.api.error.BadRecordException;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.BridgeInputBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.io.DataInputStream;
import java.util.List;

/*
 * WriteBridge class creates appropriate accessor and resolver.
 * It reads data from inputStream by the resolver,
 * and writes it to the Hadoop storage with the accessor.
 */
@Component
@RequestScope
public class WriteBridge extends BaseBridge {

    private final BridgeInputBuilder inputBuilder;

    public WriteBridge(BridgeInputBuilder inputBuilder, ApplicationContext applicationContext, RequestContext context) {
        super(applicationContext, context);
        this.inputBuilder = inputBuilder;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean beginIteration() throws Exception {
        return accessor.openForWrite();
    }

    /*
     * Read data from stream, convert it using Resolver into OneRow object, and
     * pass to WriteAccessor to write into file.
     */
    @Override
    public boolean setNext(DataInputStream inputStream) throws Exception {

        List<OneField> record = inputBuilder.makeInput(inputStream);
        if (record == null) {
            return false;
        }

        OneRow onerow = resolver.setFields(record);
        if (onerow == null) {
            return false;
        }
        if (!accessor.writeNextObject(onerow)) {
            throw new BadRecordException();
        }
        return true;
    }

    /*
     * Close the underlying resource
     */
    public void endIteration() throws Exception {
        try {
            accessor.closeForWrite();
        } catch (Exception e) {
            LOG.error("Failed to close bridge resources: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Writable getNext() {
        throw new UnsupportedOperationException("getNext is not implemented");
    }

}
