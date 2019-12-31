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

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.ArrayField;
import org.greenplum.pxf.api.ArrayStreamingField;
import org.greenplum.pxf.api.BadRecordException;
import org.greenplum.pxf.api.GreenplumDateTime;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.StreamingField;
import org.greenplum.pxf.api.io.BufferWritable;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.io.GPDBWritable;
import org.greenplum.pxf.api.io.Text;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.GreenplumCSV;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.StreamingResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.greenplum.pxf.api.io.DataType.TEXT;

/**
 * Class creates the output record that is piped by the java process to the GPDB
 * backend. Actually, the output record is serialized and the obtained byte
 * string is piped to the GPDB segment. The output record will implement
 * Writable, and the mission of BridgeOutputBuilder will be to translate a list
 * of {@link OneField} objects (obtained from the Resolver) into an output
 * record.
 */
public class BridgeOutputBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(BridgeOutputBuilder.class);

    private static final byte DELIM = 10; /* (byte)'\n'; */
    private RequestContext context;
    private Writable output = null;
    private LinkedList<Writable> outputList;
    private Writable partialLine = null;
    private GPDBWritable errorRecord = null;
    private int[] schema;
    private String[] colNames;
    private boolean samplingEnabled;
    private GreenplumCSV greenplumCSV;

    /**
     * Constructs a BridgeOutputBuilder.
     *
     * @param context input data, like requested output format and schema
     *                information
     */
    public BridgeOutputBuilder(RequestContext context) {
        this.context = context;
        greenplumCSV = context.getGreenplumCSV();
        outputList = new LinkedList<>();
        makeErrorRecord();
        samplingEnabled = (this.context.getStatsSampleRatio() > 0);
    }

    /**
     * We need a separate GPDBWritable record to represent the error record.
     * Just setting the errorFlag on the "output" GPDBWritable variable is not
     * good enough, since the GPDBWritable is built only after the first record
     * is read from the file. And if we encounter an error while fetching the
     * first record from the file, then the output member will be null. The
     * reason we cannot count on the schema to build the GPDBWritable output
     * variable before reading the first record, is because the schema does not
     * account for arrays - we cannot know from the schema the length of an
     * array. We find out only after fetching the first record.
     */
    void makeErrorRecord() {
        int[] errSchema = {TEXT.getOID()};

        if (context.getOutputFormat() != OutputFormat.GPDBWritable) {
            return;
        }

        errorRecord = new GPDBWritable(errSchema);
        errorRecord.setError(true);
    }

    /**
     * Returns the error record. If the output format is not binary, a
     * comma-delimited row will be generated.
     *
     * @param ex exception to be stored in record
     * @return error record
     */
    public Writable getErrorOutput(Exception ex) throws Exception {
        if (context.getOutputFormat() == OutputFormat.GPDBWritable) {
            errorRecord.setString(0, ex.getMessage());
            return errorRecord;
        } else {
            // Serialize error text into CSV
            // We create a row with an extra column containing the error information
            LOG.error(ex.getMessage(), ex);
            return new Text(
                    StringUtils.repeat(String.valueOf(greenplumCSV.getDelimiter()), context.getTupleDescription().size()) +
                            greenplumCSV.toCsvField(ex.getMessage(), true, true, true) +
                            greenplumCSV.getNewline());
        }
    }

    /**
     * Translates recFields (obtained from the Resolver) into an output record.
     *
     * @param recFields record fields to be serialized
     * @return list of Writable objects with serialized row
     * @throws BadRecordException if building the output record failed
     */
    public LinkedList<Writable> makeOutput(List<OneField> recFields)
            throws BadRecordException {
        if (output == null && context.getOutputFormat() == OutputFormat.GPDBWritable) {
            makeGPDBWritableOutput();
        }

        outputList.clear();

        fillOutputRecord(recFields);

        return outputList;
    }

    public LinkedList<Writable> makeVectorizedOutput(List<List<OneField>> recordsBatch) throws BadRecordException {
        outputList.clear();
        if (recordsBatch != null) {
            for (List<OneField> record : recordsBatch) {
                if (context.getOutputFormat() == OutputFormat.GPDBWritable) {
                    makeGPDBWritableOutput();
                }
                fillOutputRecord(record);
            }
        }
        return outputList;
    }


    /**
     * Can take a list of non-streaming and at most one streaming field,
     * returning a lazy iterator over the data. Only supports CSV format.
     *
     * @param fields List of fields to create an iterator over
     * @return Lazy iterator of writables for the given record list
     */
    public Iterator<Writable> makeStreamingOutput(List<OneField> fields) {
        return new Iterator<Writable>() {
            private int recordCounter = 0;
            private int numRecords = fields.size();
            private OneField field;
            private StreamingResolver resolver = null;
            boolean currentlyStreaming = false;
            String newline = greenplumCSV.getNewline();
            String quote = String.valueOf(greenplumCSV.getQuote());
            String delimiter = String.valueOf(greenplumCSV.getDelimiter());

            @Override
            public boolean hasNext() {
                if (recordCounter < numRecords) {
                    return true;
                }
                // we are on the last record, check if it's a streaming type
                if (StreamingField.class.isAssignableFrom(field.getClass()) && resolver != null) {
                    return resolver.hasNext();
                }
                return false;
            }

            @Override
            public Writable next() {
                StringBuilder sb = new StringBuilder();
                if (!currentlyStreaming && numRecords > recordCounter) {
                    // we have more fields, get the next field
                    field = fields.get(recordCounter++);
                }
                String csvDelimOrNewline = (recordCounter == numRecords) ? newline : delimiter;
                if (StreamingField.class.isAssignableFrom(field.getClass())) {
                    if (!currentlyStreaming) {
                        currentlyStreaming = true;
                        resolver = ((StreamingField) field).getResolver();
                        sb.
                                append(quote).
                                append(field.getPrefix());
                    }
                    sb.append(greenplumCSV.toCsvField(resolver.next(), false, false, true));
                    if (!resolver.hasNext()) {
                        currentlyStreaming = false;
                        resolver = null;
                        sb.
                                append(field.getSuffix()).
                                append(quote).
                                append(csvDelimOrNewline);
                    } else if (field instanceof ArrayStreamingField) {
                        // element separator for array
                        sb.append(((ArrayStreamingField) field).getSeparator());
                    }
                } else {
                    // non-streaming cases are handled by usual methods
                    sb.append(fieldToCSVElement(field)).append(csvDelimOrNewline);
                }

                return new BufferWritable(sb.toString().getBytes());
            }
        };
    }

    public Writable getPartialLine() {
        return partialLine;
    }

    /**
     * Creates the GPDBWritable object. The object is created one time and is
     * refilled from recFields for each record sent
     *
     * @return empty GPDBWritable object with set columns
     */
    GPDBWritable makeGPDBWritableOutput() {
        int num_actual_fields = context.getColumns();
        schema = new int[num_actual_fields];
        colNames = new String[num_actual_fields];

        for (int i = 0; i < num_actual_fields; i++) {
            schema[i] = context.getColumn(i).columnTypeCode();
            colNames[i] = context.getColumn(i).columnName();
        }

        output = new GPDBWritable(schema);

        return (GPDBWritable) output;
    }

    /**
     * Fills the output record based on the fields in recFields.
     *
     * @param recFields record fields
     * @throws BadRecordException if building the output record failed
     */
    void fillOutputRecord(List<OneField> recFields) throws BadRecordException {
        if (context.getOutputFormat() == OutputFormat.GPDBWritable) {
            fillGPDBWritable(recFields);
        } else {
            fillText(recFields);
        }
    }

    /**
     * Fills a GPDBWritable object based on recFields. The input record
     * recFields must correspond to schema. If the record has more or less
     * fields than the schema we throw an exception. We require that the type of
     * field[i] in recFields corresponds to the type of field[i] in the schema.
     *
     * @param recFields record fields
     * @throws BadRecordException if building the output record failed
     */
    void fillGPDBWritable(List<OneField> recFields) throws BadRecordException {
        int size = recFields.size();
        if (size == 0) { // size 0 means the resolver couldn't deserialize any
            // of the record fields
            throw new BadRecordException("No fields in record");
        } else if (size != schema.length) {
            throw new BadRecordException("Record has " + size
                    + " fields but the schema size is " + schema.length);
        }

        for (int i = 0; i < size; i++) {
            OneField current = recFields.get(i);
            if (!isTypeInSchema(current.type, schema[i])) {
                throw new BadRecordException("For field " + colNames[i]
                        + " schema requires type "
                        + DataType.get(schema[i]).toString()
                        + " but input record has type "
                        + DataType.get(current.type).toString());
            }

            fillOneGPDBWritableField(current, i);
        }

        outputList.add(output);
    }

    /**
     * Tests if data type is a string type. String type is a type that can be
     * serialized as string, such as varchar, bpchar, text, numeric, timestamp,
     * date.
     *
     * @param type data type
     * @return whether data type is string type
     */
    boolean isStringType(DataType type) {
        return Arrays.asList(
                DataType.VARCHAR,
                DataType.BPCHAR,
                DataType.TEXT,
                DataType.NUMERIC,
                DataType.TIMESTAMP,
                DataType.TIMESTAMP_WITH_TIME_ZONE,
                DataType.DATE)
                .contains(type);
    }

    /**
     * Tests if record field type and schema type correspond.
     *
     * @param recType    record type code
     * @param schemaType schema type code
     * @return whether record type and schema type match
     */
    boolean isTypeInSchema(int recType, int schemaType) {
        DataType dtRec = DataType.get(recType);
        DataType dtSchema = DataType.get(schemaType);

        return (dtSchema == DataType.UNSUPPORTED_TYPE || dtRec == dtSchema || (isStringType(dtRec) && isStringType(dtSchema)));
    }

    /**
     * Fills a Text object based on recFields.
     *
     * @param recFields record fields
     * @throws BadRecordException if text formatted record has more than one
     *                            field
     */
    void fillText(List<OneField> recFields) throws BadRecordException {
        if (recFields.size() < 1)
            throw new BadRecordException(
                    "BridgeOutputBuilder must receive one field when handling the TEXT format");

        OneField field = recFields.get(0);
        Object val = field.val;
        DataType dataType = DataType.get(field.type);

        if (recFields.size() == 1 && dataType == DataType.BYTEA) {
            if (samplingEnabled) {
                convertTextDataToLines((byte[]) val);
                return;
            } else {
                // TODO break output into lines
                output = new BufferWritable((byte[]) val);
            }
        } else {
            String textRec = (recFields.size() == 1 && val instanceof String) ?
                    val + greenplumCSV.getNewline() :
                    fieldListToCSVString(recFields);
            output = new Text(textRec);
        }

        outputList.add(output);
    }

    /**
     * Breaks raw bytes into lines. Used only for sampling.
     * <p>
     * When sampling a data source, we have to make sure that we deal with
     * actual rows (lines) and not bigger chunks of data such as used by
     * LineBreakAccessor for performance. The input byte array is broken into
     * lines, each one stored in the outputList. In case the read data doesn't
     * end with a line delimiter, which can happen when reading chunks of bytes,
     * the partial line is stored separately, and is being completed when
     * reading the next chunk of data.
     *
     * @param val input raw data to break into lines
     */
    void convertTextDataToLines(byte[] val) {
        int len = val.length;
        int start = 0;
        int end;
        byte[] line;
        BufferWritable writable;

        while (start < len) {
            end = ArrayUtils.indexOf(val, DELIM, start);
            boolean isPartialLine = false;
            if (end == ArrayUtils.INDEX_NOT_FOUND) {
                // data finished in the middle of the line
                end = len;
                isPartialLine = true;
            } else {
                end++; // include the DELIM character
                isPartialLine = false;
            }
            line = Arrays.copyOfRange(val, start, end);

            if (partialLine != null) {
                // partial data was completed
                ((BufferWritable) partialLine).append(line);
                writable = (BufferWritable) partialLine;
                partialLine = null;
            } else {
                writable = new BufferWritable(line);
            }

            if (isPartialLine) {
                partialLine = writable;
            } else {
                outputList.add(writable);
            }
            start = end;
        }
    }

    /**
     * Fills one GPDBWritable field.
     *
     * @param oneField field
     * @param colIdx   column index
     * @throws BadRecordException if field type is not supported or doesn't
     *                            match the schema
     */
    void fillOneGPDBWritableField(OneField oneField, int colIdx)
            throws BadRecordException {
        int type = oneField.type;
        Object val = oneField.val;
        GPDBWritable gpdbOutput = (GPDBWritable) output;
        try {
            switch (DataType.get(type)) {
                case INTEGER:
                    gpdbOutput.setInt(colIdx, (Integer) val);
                    break;
                case FLOAT8:
                    gpdbOutput.setDouble(colIdx, (Double) val);
                    break;
                case REAL:
                    gpdbOutput.setFloat(colIdx, (Float) val);
                    break;
                case BIGINT:
                    gpdbOutput.setLong(colIdx, (Long) val);
                    break;
                case SMALLINT:
                    gpdbOutput.setShort(colIdx, (Short) val);
                    break;
                case BOOLEAN:
                    gpdbOutput.setBoolean(colIdx, (Boolean) val);
                    break;
                case BYTEA:
                    byte[] bts = null;
                    if (val != null) {
                        int length = Array.getLength(val);
                        bts = new byte[length];
                        for (int j = 0; j < length; j++) {
                            bts[j] = Array.getByte(val, j);
                        }
                    }
                    gpdbOutput.setBytes(colIdx, bts);
                    break;
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                case NUMERIC:
                case TIMESTAMP:
                case DATE:
                    gpdbOutput.setString(colIdx,
                            ObjectUtils.toString(val, null));
                    break;
                default:
                    String valClassName = (val != null) ? val.getClass().getSimpleName()
                            : null;
                    throw new UnsupportedOperationException(valClassName
                            + " is not supported for GPDB conversion");
            }
        } catch (GPDBWritable.TypeMismatchException e) {
            throw new BadRecordException(e);
        }
    }

    /**
     * Serialize a list of OneFields to a CSV line
     *
     * @param fields list of fields
     * @return a serialized CSV line
     */
    private String fieldListToCSVString(List<OneField> fields) {
        return fields.stream()
                .map(this::fieldToCSVElement)
                .collect(Collectors.joining(String.valueOf(greenplumCSV.getDelimiter()), "", greenplumCSV.getNewline()));
    }

    private String fieldToCSVElement(OneField field) {
        if (field.val == null)
            return greenplumCSV.getValueOfNull();
        else if (field.type == DataType.BYTEA.getOID())
            return "\\x" + Hex.encodeHexString((byte[]) field.val);
        else if (field.type == DataType.NUMERIC.getOID() || !DataType.isTextForm(field.type))
            return Objects.toString(field.val, null);
        else if (field.type == DataType.TIMESTAMP.getOID())
            return ((Timestamp) field.val).toLocalDateTime().format(GreenplumDateTime.DATETIME_FORMATTER);
        else if (field.type == DataType.DATE.getOID())
            return field.val.toString();
        else if (field instanceof ArrayField) {
            List<?> list = (List<?>) field.val;
            String arrayString = ((ArrayField) field).getPrefix() +
                    list.stream().map(Object::toString).collect(Collectors.joining(((ArrayField) field).getSeparator())) +
                    ((ArrayField) field).getSuffix();
            return greenplumCSV.toCsvField(arrayString, true, true, true);
        }
        return greenplumCSV.toCsvField((String) field.val, true, true, true);
    }
}
