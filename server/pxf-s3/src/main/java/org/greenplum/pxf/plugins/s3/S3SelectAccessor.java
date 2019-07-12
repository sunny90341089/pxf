package org.greenplum.pxf.plugins.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CSVInput;
import com.amazonaws.services.s3.model.CSVOutput;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.JSONInput;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.ParquetInput;
import com.amazonaws.services.s3.model.SelectObjectContentEvent;
import com.amazonaws.services.s3.model.SelectObjectContentEventVisitor;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Accessor to read data from S3, using the S3 Select Framework.
 * S3 Select works on a single key (or object), pushing down as
 * much computation as possible to S3. This reduces the amount of
 * data we transfer over the wire, with the purpose of speeding up
 * query times from S3.
 */
public class S3SelectAccessor extends BasePlugin implements Accessor {

    // We call this option compression_codec to make it compatible to
    // the COMPRESSION_CODECs from the s3:text, s3:parquet profiles
    public static final String COMPRESSION_TYPE = "COMPRESSION_CODEC";
    // Keep the same name as the FORMAT option in Greenplum
    public static final String FILE_HEADER_INFO = "HEADER";
    public static final String FILE_HEADER_INFO_NONE = "NONE";
    public static final String FILE_HEADER_INFO_IGNORE = "IGNORE";
    public static final String FILE_HEADER_INFO_USE = "USE";
    // Keep the same name as the FORMAT option in Greenplum
    public static final String FIELD_DELIMITER = "DELIMITER";
    // Keep the same name as the FORMAT option in Greenplum
    public static final String QUOTE_ESCAPE_CHARACTER = "ESCAPE";
    // Keep the same name as the FORMAT option in Greenplum
    public static final String RECORD_DELIMITER = "NEWLINE";
    // Keep the same name as the FORMAT option in Greenplum
    public static final String QUOTE_CHARACTER = "QUOTE";
    public static final String JSON_TYPE = "JSON-TYPE";
    private AtomicBoolean isResultComplete;
    private AmazonS3 s3Client;
    private SelectObjectContentResult result;
    private InputStream resultInputStream;
    private BufferedReader reader;
    private int lineReadCount;
    private URI name;

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);

        name = URI.create(context.getDataSource());
        s3Client = initS3Client();
        lineReadCount = 0;
    }

    @Override
    public boolean openForRead() {
        isResultComplete = new AtomicBoolean(false);
        SelectObjectContentRequest request = generateBaseCSVRequest(context);

        result = s3Client.selectObjectContent(request);
        resultInputStream = result.getPayload().getRecordsInputStream(
                new SelectObjectContentEventVisitor() {
                    @Override
                    public void visit(SelectObjectContentEvent.StatsEvent event) {
                        LOG.debug("Received Stats, Bytes Scanned: {}. Bytes Processed: {}",
                                event.getDetails().getBytesScanned(),
                                event.getDetails().getBytesProcessed());
                    }

                    /*
                     * An End Event informs that the request has finished successfully.
                     */
                    @Override
                    public void visit(SelectObjectContentEvent.EndEvent event) {
                        isResultComplete.set(true);
                        LOG.debug("Received End Event. Result is complete.");
                    }
                }
        );
        reader = new BufferedReader(new InputStreamReader(resultInputStream));
        return resultInputStream != null;
    }

    /**
     * Reads one line at a time
     *
     * @return the next line, or null if the EOF has been reached
     */
    @Override
    public OneRow readNextObject() throws Exception {
        String str = reader.readLine();
        if (str != null) {
            lineReadCount++;
            return new OneRow(null, str);
        }
        return null;
    }

    @Override
    public void closeForRead() throws Exception {
        LOG.debug("Read {} lines", lineReadCount);
        /*
         * Make sure to close all streams
         */
        if (result != null) {
            try {
                result.close();
                LOG.debug("SelectObjectContentResult closed");
            } catch (IOException e) {
                LOG.error("Unable to close SelectObjectContentResult", e);
            }
        }

        if (resultInputStream != null) {
            try {
                resultInputStream.close();
                LOG.debug("ResultInputStream closed");
            } catch (IOException e) {
                LOG.error("Unable to close ResultInputStream", e);
            }
        }

        /*
         * The End Event indicates all matching records have been transmitted.
         * If the End Event is not received, the results may be incomplete.
         */
        if (!isResultComplete.get()) {
            throw new RuntimeException("S3 Select request was incomplete as End Event was not received.");
        }
    }

    /**
     * Generates the {@link SelectObjectContentRequest} object from
     * the request context.
     *
     * @param context the request context
     * @return a {@link SelectObjectContentRequest}
     */
    SelectObjectContentRequest generateBaseCSVRequest(RequestContext context) {

        InputSerialization inputSerialization = getInputSerialization(context);

        String fileHeaderInfo = context.getOption(FILE_HEADER_INFO);
        boolean usePositionToIdentifyColumn = inputSerialization.getCsv() != null &&
                (StringUtils.isBlank(fileHeaderInfo) ||
                        StringUtils.equalsIgnoreCase(FILE_HEADER_INFO_NONE, fileHeaderInfo) ||
                        StringUtils.equalsIgnoreCase(FILE_HEADER_INFO_IGNORE, fileHeaderInfo));
        S3SelectQueryBuilder queryBuilder = new S3SelectQueryBuilder(context, usePositionToIdentifyColumn);
        String query = queryBuilder.buildSelectQuery();

        LOG.trace("Select query: {}", query);

        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(name.getHost());
        request.setKey(StringUtils.removeStart(name.getPath(), "/"));
        request.setExpression(query);
        request.setExpressionType(ExpressionType.SQL);

        LOG.debug("With bucket name '{}'", request.getBucketName());
        LOG.debug("With key '{}'", request.getKey());
        LOG.debug("With expression query '{}'", query);

        request.setInputSerialization(inputSerialization);

        OutputSerialization outputSerialization = getOutputSerialization(context);
        request.setOutputSerialization(outputSerialization);

        return request;
    }

    /**
     * Returns a {@link com.amazonaws.services.s3.model.OutputSerialization}
     * object with parsed values from the request context.
     *
     * @param context the request context
     * @return a {@link OutputSerialization} object
     */
    private OutputSerialization getOutputSerialization(RequestContext context) {
        OutputSerialization outputSerialization = new OutputSerialization();
        CSVOutput csvOutput = new CSVOutput();
        // TODO: FDW: In FDW world, these should be passed as part of the request
        csvOutput.setQuoteCharacter("\"");
        csvOutput.setQuoteEscapeCharacter("\"");
        outputSerialization.setCsv(csvOutput);
        return outputSerialization;
    }

    /**
     * Returns a {@link com.amazonaws.services.s3.model.InputSerialization}
     * object with parsed values from the request context.
     *
     * @param context the request context
     * @return a {@link InputSerialization} object
     */
    InputSerialization getInputSerialization(RequestContext context) {
        InputSerialization inputSerialization = new InputSerialization();

        String format = context.getFormat();
        String compressionType = context.getOption(COMPRESSION_TYPE);

        LOG.debug("With format {}", format);
        if (StringUtils.equalsIgnoreCase(format, "parquet")) {
            inputSerialization.setParquet(new ParquetInput());
        } else if (StringUtils.equalsIgnoreCase(format, "json")) {
            inputSerialization.setJson(getJSONInput(context));
        } else {
            inputSerialization.setCsv(getCSVInput(context));
        }

        LOG.debug("With compression type {}", compressionType);
        if (StringUtils.equalsIgnoreCase(compressionType, "gzip")) {
            inputSerialization.setCompressionType(CompressionType.GZIP);
        } else if (StringUtils.equalsIgnoreCase(compressionType, "bzip2")) {
            inputSerialization.setCompressionType(CompressionType.BZIP2);
        } else {
            inputSerialization.setCompressionType(CompressionType.NONE);
        }

        return inputSerialization;
    }

    /**
     * Returns a {@link com.amazonaws.services.s3.model.JSONInput}
     * object with parsed values from the request context.
     *
     * @param context the request context
     * @return a {@link com.amazonaws.services.s3.model.JSONInput}
     */
    JSONInput getJSONInput(RequestContext context) {
        JSONInput jsonInput = new JSONInput();

        String jsonType = context.getOption(JSON_TYPE);

        if (StringUtils.isNoneBlank(jsonType)) {
            jsonInput.setType(jsonType);
        }
        return jsonInput;
    }

    /**
     * Returns a {@link com.amazonaws.services.s3.model.CSVInput}
     * object with parsed values from the request context.
     *
     * @param context the request context
     * @return a {@link CSVInput}
     */
    CSVInput getCSVInput(RequestContext context) {
        CSVInput csvInput = new CSVInput();

        String fileHeaderInfo = context.getOption(FILE_HEADER_INFO);
        if (fileHeaderInfo != null) {
            LOG.debug("With CSV FileHeaderInfo '{}'", fileHeaderInfo);
            csvInput.setFileHeaderInfo(fileHeaderInfo);
        }

        String fieldDelimiter = context.getOption(FIELD_DELIMITER);
        if (fieldDelimiter != null) {
            LOG.debug("With CSV field delimiter '{}'", fieldDelimiter);
            csvInput.setFieldDelimiter(fieldDelimiter);
        }

        String quoteEscapeCharacter = context.getOption(QUOTE_ESCAPE_CHARACTER);
        if (quoteEscapeCharacter != null) {
            LOG.debug("With CSV quote escape character '{}'", quoteEscapeCharacter);
            csvInput.setQuoteEscapeCharacter(quoteEscapeCharacter);
        }

        String recordDelimiter = context.getOption(RECORD_DELIMITER);
        if (recordDelimiter != null) {
            LOG.debug("With CSV record delimiter '{}'", recordDelimiter);
            csvInput.setRecordDelimiter(recordDelimiter);
        }

        String quoteCharacter = context.getOption(QUOTE_CHARACTER);
        if (quoteCharacter != null) {
            LOG.debug("With CSV quote character '{}'", quoteCharacter);
            csvInput.setQuoteCharacter(quoteCharacter);
        }

        return csvInput;
    }

    /**
     * Returns a new AmazonS3 client with credentials from
     * the configuration file
     */
    private AmazonS3 initS3Client() {
        try {
            DefaultS3ClientFactory factory = new DefaultS3ClientFactory();
            factory.setConf(configuration);
            return factory.createS3Client(name);
        } catch (IOException e) {
            throw new RuntimeException("Unable to create S3 Client connection", e);
        }
    }

    @Override
    public boolean openForWrite() {
        throw new UnsupportedOperationException("S3 Select does not support writing");
    }

    @Override
    public boolean writeNextObject(OneRow onerow) {
        throw new UnsupportedOperationException("S3 Select does not support writing");
    }

    @Override
    public void closeForWrite() {
        throw new UnsupportedOperationException("S3 Select does not support writing");
    }
}