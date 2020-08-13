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

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.Resolver;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.util.LinkedList;
import java.util.List;

/**
 * Class that defines the deserialization of one record brought from the external input data.
 * <p>
 * Demo implementation that returns record custom format
 */
@Component("DemoResolver")
@RequestScope
public class DemoResolver extends BasePlugin implements Resolver {

    /**
     * Read the next record
     * The record contains as many fields as defined by the DDL schema.
     *
     * @param row one record
     * @return list of fields or columns
     */
    @Override
    public List<OneField> getFields(OneRow row) {
        List<OneField> output = new LinkedList<>();
        Object data = row.getData();

        /* break up the row into fields */
        String[] fields = ((String) data).split(",");
        for (String field : fields) {
            output.add(new OneField(DataType.VARCHAR.getOID(), field));
        }

        return output;
    }

    /**
     * Constructs and sets the fields of a {@link OneRow}.
     *
     * @param record list of {@link OneField}
     * @return the constructed {@link OneRow}
     * @throws Exception if constructing a row from the fields failed
     */
    @Override
    public OneRow setFields(List<OneField> record) throws Exception {
        throw new UnsupportedOperationException();
    }
}
