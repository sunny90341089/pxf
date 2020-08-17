package org.greenplum.pxf.automation.features.writable;

import annotations.ExpectedFailure;
import org.greenplum.pxf.automation.datapreparer.writable.WritableDataPreparer;
import org.greenplum.pxf.automation.enums.EnumCompressionTypes;
import org.greenplum.pxf.automation.features.BaseWritableFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.fileformats.FileFormatsUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.greenplum.pxf.automation.utils.tables.ComparisonUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.Timestamp;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

import static java.lang.Thread.sleep;

/**
 * Testing cases for PXF Writable feature for Text formats (Text, CSV) and compressions.
 */
public class HdfsWritableTextTest extends BaseWritableFeature {

    private static final String COMPRESSION_CODEC = "org.apache.hadoop.io.compress.DefaultCodec";
    private Table dataTable;
    private String[] gpdbTableFields;
    private String hdfsWorkingDataDir;

    private enum InsertionMethod {
        INSERT,
        COPY,
        INSERT_FROM_TABLE
    }

    @Override
    protected void beforeClass() throws Exception {
        super.beforeClass();
        gpdbTableFields = new String[]{
                "t1    TEXT",
                "bi    BIGINT",
                "b     BIT",
                "bool  BOOLEAN",
                "int   INTEGER",
                "si    SMALLINT",
                "bin   BYTEA",
                "ts    TIMESTAMP",
                "circ  CIRCLE"
        };
        dataTable = new Table("data_table", null);
        // prepare small data stored in dataTable
        WritableDataPreparer dataPreparer = new WritableDataPreparer();
        dataPreparer.prepareData(100, dataTable);
        hdfsWorkingDataDir = hdfs.getWorkingDirectory() + "/data";
    }

    @Override
    protected void afterMethod() throws Exception {
        super.afterMethod();
    }

    @Override
    protected void beforeMethod() {
        writableExTable = TableFactory.getPxfWritableTextTable(writableTableName,
                gpdbTableFields, hdfsWritePath + writableTableName, ",");
        writableExTable.setHost(pxfHost);
        writableExTable.setPort(pxfPort);

        readableExTable = TableFactory.getPxfReadableTextTable(readableTableName,
                gpdbTableFields, hdfsWritePath + writableTableName, ",");
        readableExTable.setHost(pxfHost);
        readableExTable.setPort(pxfPort);
    }

    /**
     * Insert data to Writable table using specific plugins in the table
     * location instead of using profile.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "security"})
    public void textFormatInsertNoProfile() throws Exception {

        writableExTable.setProfile(null);
        writableExTable.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        writableExTable.setAccessor("org.greenplum.pxf.plugins.hdfs.LineBreakAccessor");
        writableExTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        gpdb.createTableAndVerify(writableExTable);

        insertData(dataTable, writableExTable, InsertionMethod.INSERT);
        verifyResult(hdfsWritePath + writableTableName, dataTable);
    }

    /**
     * Insert data using "THREAD-SAFE=FALSE" option literally
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void textFormatInsertThreadSafeFalse() throws Exception {

        writableExTable.setUserParameters(new String[]{"THREAD-SAFE=FALSE"});
        gpdb.createTableAndVerify(writableExTable);

        insertData(dataTable, writableExTable, InsertionMethod.INSERT);
        verifyResult(hdfsWritePath + writableTableName, dataTable);
    }

    /**
     * Insert data using 2 Writable tables using "THREAD-SAFE=FALSE" and the
     * other "THREAD-SAFE=TRUE"
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void textFormatInsertDifferentThreadSafeStates() throws Exception {

        WritableExternalTable writableExTableFalseThreadSafe = TableFactory.getPxfWritableTextTable(
                writableTableName, gpdbTableFields, hdfsWritePath + writableTableName, ",");
        writableExTableFalseThreadSafe.setUserParameters(new String[]{"THREAD-SAFE=FALSE"});
        writableExTableFalseThreadSafe.setHost(pxfHost);
        writableExTableFalseThreadSafe.setPort(pxfPort);
        gpdb.createTableAndVerify(writableExTable);

        insertData(dataTable, writableExTable, InsertionMethod.INSERT);
        insertData(dataTable, writableExTableFalseThreadSafe, InsertionMethod.INSERT);
        Table pumpedDataTable = new Table("pumped_data_table", null);
        pumpedDataTable.setData(dataTable.getData());
        pumpedDataTable.pumpUpTableData(2);
        verifyResult(hdfsWritePath + writableTableName, pumpedDataTable);
    }

    /**
     * Insert data using "org.apache.hadoop.io.compress.DefaultCodec" codec.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void textFormatInsertDefaultCodec() throws Exception {

        writableExTable.setCompressionCodec(COMPRESSION_CODEC);
        gpdb.createTableAndVerify(writableExTable);
        insertData(dataTable, writableExTable, InsertionMethod.INSERT);

        // for HCFS on Cloud, wait a bit for async write in previous steps to finish
        sleep(10000);

        gpdb.createTableAndVerify(readableExTable);
        gpdb.queryResults(readableExTable,
                "SELECT * FROM " + readableExTable.getName() + " ORDER BY bi");
        ComparisonUtils.compareTables(dataTable, readableExTable, null, "\\\\");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void textFormatCopyDefaultCodec() throws Exception {

        writableExTable.setCompressionCodec(COMPRESSION_CODEC);
        gpdb.createTableAndVerify(writableExTable);
        insertData(dataTable, writableExTable, InsertionMethod.COPY);

        // for HCFS on Cloud, wait a bit for async write in previous steps to finish
        sleep(10000);

        gpdb.createTableAndVerify(readableExTable);
        gpdb.queryResults(readableExTable,
                "SELECT * FROM " + readableExTable.getName() + " ORDER BY bi");
        ComparisonUtils.compareTables(dataTable, readableExTable, null, "\\\\");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void textFormatInsertFromTableDefaultCodec() throws Exception {

        // Generate data to HDFS, create Readable table pointing to HDFS data
        hdfs.writeTableToFile(hdfsWorkingDataDir, dataTable, ",");
        readableExTable.setPath(hdfsWorkingDataDir);
        gpdb.createTableAndVerify(readableExTable);

        // create Writable table and insert data from the Readable table
        writableExTable.setCompressionCodec(COMPRESSION_CODEC);
        gpdb.createTableAndVerify(writableExTable);

        insertData(readableExTable, writableExTable, InsertionMethod.INSERT_FROM_TABLE);

        // for HCFS on Cloud, wait a bit for async write in previous steps to finish
        sleep(10000);

        // create another Readable table to verify the data
        readableExTable.setPath(hdfsWritePath + writableTableName);
        gpdb.createTableAndVerify(readableExTable);
        gpdb.queryResults(readableExTable,
                "SELECT * FROM " + readableExTable.getName() + " ORDER BY bi");
        ComparisonUtils.compareTables(dataTable, readableExTable, null, "\\\\");
    }

    /**
     * Insert plain text data
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void textFormatInsert() throws Exception {

        gpdb.createTableAndVerify(writableExTable);
        insertData(dataTable, writableExTable, InsertionMethod.INSERT);
        verifyResult(hdfsWritePath + writableTableName, dataTable);
    }

    /**
     * Copy plain text data
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void textFormatCopyFromStdin() throws Exception {

        gpdb.createTableAndVerify(writableExTable);
        insertData(dataTable, writableExTable, InsertionMethod.COPY);
        verifyResult(hdfsWritePath + writableTableName, dataTable);
    }

    /**
     * Insert plain text data from readable table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void textFormatInsertFromTable() throws Exception {

        hdfs.writeTableToFile(hdfsWorkingDataDir, dataTable, ",");
        readableExTable.setPath(hdfsWorkingDataDir);
        gpdb.createTableAndVerify(readableExTable);
        gpdb.createTableAndVerify(writableExTable);

        insertData(readableExTable, writableExTable, InsertionMethod.INSERT_FROM_TABLE);
        verifyResult(hdfsWritePath + writableTableName, dataTable);
    }

    /**
     * Insert plain text data using CSV format
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void csvFormatInsert() throws Exception {

        String hdfsPath = hdfsWritePath + writableTableName + "_csv";
        writableExTable.setPath(hdfsPath);
        writableExTable.setFormat("CSV");
        gpdb.createTableAndVerify(writableExTable);

        insertData(dataTable, writableExTable, InsertionMethod.INSERT);
        verifyResult(hdfsPath, dataTable);
    }

    /**
     * Copy plain text data using CSV format
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void csvFormatCopyFromStdin() throws Exception {

        String hdfsPath = hdfsWritePath + writableTableName + "_csv";
        writableExTable.setPath(hdfsPath);
        writableExTable.setFormat("CSV");
        gpdb.createTableAndVerify(writableExTable);

        insertData(dataTable, writableExTable, InsertionMethod.COPY);
        verifyResult(hdfsPath, dataTable);
    }

    /**
     * Insert plain text data from readable table using CSV format
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void csvFormatInsertFromTable() throws Exception {

        hdfs.writeTableToFile(hdfsWorkingDataDir, dataTable, ",");
        readableExTable.setPath(hdfsWorkingDataDir);
        gpdb.createTableAndVerify(readableExTable);

        String hdfsPath = hdfsWritePath + writableTableName + "_csv";
        writableExTable.setPath(hdfsPath);
        writableExTable.setFormat("CSV");
        createTable(writableExTable);

        insertData(readableExTable, writableExTable, InsertionMethod.INSERT_FROM_TABLE);
        verifyResult(hdfsPath, dataTable);
    }

    /**
     * Insert plain text data using GZip codec
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void textFormatGZipInsert() throws Exception {

        String hdfsPath = hdfsWritePath + writableTableName + "_gzip";
        writableExTable = TableFactory.getPxfWritableGzipTable(writableTableName,
                gpdbTableFields, hdfsPath, ",");
        createTable(writableExTable);

        insertData(dataTable, writableExTable, InsertionMethod.INSERT);
        verifyResult(hdfsPath, dataTable, EnumCompressionTypes.GZip);
    }

    /**
     * Copy plain text data using GZip codec
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void textFormatGZipCopyFromStdin() throws Exception {

        String hdfsPath = hdfsWritePath + writableTableName + "_gzip";
        writableExTable = TableFactory.getPxfWritableGzipTable(writableTableName,
                gpdbTableFields, hdfsPath, ",");
        createTable(writableExTable);

        insertData(dataTable, writableExTable, InsertionMethod.COPY);
        verifyResult(hdfsPath, dataTable, EnumCompressionTypes.GZip);
    }

    /**
     * Insert plain text data from readable table using GZip codec
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void textFormatGZipInsertFromTable() throws Exception {

        hdfs.writeTableToFile(hdfsWorkingDataDir, dataTable, ",");
        readableExTable.setPath(hdfsWorkingDataDir);
        gpdb.createTableAndVerify(readableExTable);

        String hdfsPath = hdfsWritePath + writableTableName + "_gzip";
        writableExTable = TableFactory.getPxfWritableGzipTable(writableTableName,
                gpdbTableFields, hdfsPath, ",");
        createTable(writableExTable);

        insertData(readableExTable, writableExTable, InsertionMethod.INSERT_FROM_TABLE);
        verifyResult(hdfsPath, dataTable, EnumCompressionTypes.GZip);
    }

    /**
     * Insert plain text data using BZip2 codec
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void textFormatBZip2Insert() throws Exception {

        String hdfsPath = hdfsWritePath + writableTableName + "_bzip2";
        writableExTable = TableFactory.getPxfWritableBZip2Table(writableTableName,
                gpdbTableFields, hdfsPath, ",");
        createTable(writableExTable);

        insertData(dataTable, writableExTable, InsertionMethod.INSERT);
        verifyResult(hdfsPath, dataTable, EnumCompressionTypes.BZip2);
    }

    /**
     * Copy plain text data using BZip2 codec
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void textFormatBZip2CopyFromStdin() throws Exception {

        String hdfsPath = hdfsWritePath + writableTableName + "_bzip2";
        writableExTable = TableFactory.getPxfWritableBZip2Table(writableTableName,
                gpdbTableFields, hdfsPath, ",");
        createTable(writableExTable);

        insertData(dataTable, writableExTable, InsertionMethod.COPY);
        verifyResult(hdfsPath, dataTable, EnumCompressionTypes.BZip2);
    }

    /**
     * Copy plain text data from very wide rows
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"}, timeOut = 120000)
    public void textFormatWideRowsInsert() throws Exception {

        int rows = 10;
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 10 * 1024 * 1024;
        Random random = new Random();

        String hdfsPath = hdfsWritePath + writableTableName + "_wide_row";
        writableExTable.setName(writableTableName + "_wide_row_w");
        writableExTable.setPath(hdfsPath);
        createTable(writableExTable);

        dataTable = new Table("data_table", null);
        int timeZoneOffset = TimeZone.getDefault().getRawOffset();
        for (int j = 0; j < rows; j++) {

            // Generate a large string to insert
            String generatedString = random.ints(leftLimit, rightLimit + 1)
                    .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                    .limit(targetStringLength)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();

            dataTable.addRow(new String[]{
                    generatedString,
                    String.valueOf(j + 1000),
                    (((j % 2) == 0) ? "1" : "0"),
                    (((j % 2) == 0) ? "t" : "f"),
                    String.valueOf(j + 1000),
                    String.valueOf(j + 10),
                    ("b#!?bbb_" + (j + 1)),
                    new Timestamp((System.currentTimeMillis()) - timeZoneOffset).toString(),
                    ("<(" + (j + 1) + "\\," + (j + 1) + ")\\," + (j + 1)) + ">"});
        }

        insertData(dataTable, writableExTable, InsertionMethod.INSERT);
        verifyResult(hdfsPath, dataTable);
    }

    /**
     * Insert plain text data from readable table using BZip2 codec
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void textFormatBZip2InsertFromTable() throws Exception {

        hdfs.writeTableToFile(hdfsWorkingDataDir, dataTable, ",");
        readableExTable.setPath(hdfsWorkingDataDir);
        gpdb.createTableAndVerify(readableExTable);

        String hdfsPath = hdfsWritePath + writableTableName + "_bzip2";
        writableExTable = TableFactory.getPxfWritableBZip2Table(writableTableName,
                gpdbTableFields, hdfsPath, ",");
        createTable(writableExTable);

        insertData(readableExTable, writableExTable, InsertionMethod.INSERT_FROM_TABLE);
        verifyResult(hdfsPath, dataTable, EnumCompressionTypes.BZip2);
    }

    /**
     * Copy plain text multi blocked data from file.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void copyFromFileMultiBlockedDataNoCompression() throws Exception {

        Table data = new Table("data", null);
        FileFormatsUtils.prepareData(new WritableDataPreparer(), 1000, data);
        // multiple it to file
        String multiBlockedLocalFilePath = dataTempFolder + "/multiBlockedData";
        FileFormatsUtils.prepareDataFile(data, 15000, multiBlockedLocalFilePath);

        String hdfsPath = hdfsWritePath + writableTableName + "_multi_block";
        writableExTable.setPath(hdfsPath);
        gpdb.createTableAndVerify(writableExTable);

        gpdb.copyFromFile(writableExTable, new File(multiBlockedLocalFilePath), ",", false);

        // for HCFS on Cloud, wait a bit for async write in previous steps to finish
        sleep(10000);

        readableExTable.setPath(hdfsPath);
        gpdb.createTableAndVerify(readableExTable);
        gpdb.runAnalyticQuery("SELECT COUNT(*) FROM " + readableExTable.getName(),
                String.valueOf(1000 * 15000));
    }

    /**
     * Copy plain text multi blocked data from file using GZip codec.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void copyFromFileMultiBlockedDataGZip() throws Exception {

        Table data = new Table("data", null);
        FileFormatsUtils.prepareData(new WritableDataPreparer(), 1000, data);
        // multiple it to file
        String multiBlockedLocalFilePath = (dataTempFolder + "/multiBlockedData_gzip");
        FileFormatsUtils.prepareDataFile(data, 15000, multiBlockedLocalFilePath);

        String hdfsPath = hdfsWritePath + writableTableName + "_multi_block_gzip";
        writableExTable = TableFactory.getPxfWritableGzipTable(writableTableName,
                gpdbTableFields, hdfsPath, ",");
        createTable(writableExTable);

        gpdb.copyFromFile(writableExTable, new File(multiBlockedLocalFilePath), ",", false);

        // for HCFS on Cloud, wait a bit for async write in previous steps to finish
        sleep(10000);

        readableExTable.setPath(hdfsPath);
        gpdb.createTableAndVerify(readableExTable);
        gpdb.runAnalyticQuery("SELECT COUNT(*) FROM " + readableExTable.getName(),
                String.valueOf(1000 * 15000));
    }

    /**
     * Copy plain text multi blocked data from file using BZip2 codec.
     * <p>
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void copyFromFileMultiBlockedDataBZip2() throws Exception {

        Table data = new Table("data", null);
        FileFormatsUtils.prepareData(new WritableDataPreparer(), 1000, data);
        // multiple it to file
        String multiBlockedLocalFilePath = dataTempFolder + "/multiBlockedData_bzip";
        FileFormatsUtils.prepareDataFile(data, 15000, multiBlockedLocalFilePath);

        String hdfsPath = hdfsWritePath + writableTableName + "_multi_block_bzip";
        writableExTable = TableFactory.getPxfWritableBZip2Table(writableTableName,
                gpdbTableFields, hdfsPath, ",");
        writableExTable.setUserParameters(new String[]{"THREAD-SAFE=FALSE"});
        createTable(writableExTable);

        gpdb.copyFromFile(writableExTable, new File(multiBlockedLocalFilePath), ",", false);

        if (ProtocolUtils.getProtocol() != ProtocolEnum.HDFS) {
            // for HCFS on Cloud, wait a bit for async write in previous steps to finish
            sleep(10000);
        }

        readableExTable.setPath(hdfsPath);
        gpdb.createTableAndVerify(readableExTable);
        gpdb.runAnalyticQuery("SELECT COUNT(*) FROM " + readableExTable.getName(),
                String.valueOf(1000 * 15000));
    }

    /**
     * Verify GPSQL-2657, handling of ~16k record followed by a short record.
     * <p>
     * We create the writable external table with a distribution key. The reason
     * for that is to make sure all data is processed by the same segment.
     * Notice we use the key "alwayssamekey".
     * <p>
     * The test creates a writable external table, copies the data into it, then
     * uses a readable external table to compare the data with the original.
     */
    @Test(groups = {"features"})
    public void veryLongRecords() throws Exception {

        final String[][] data = new String[][]{
                {"alwayssamekey", "1", StringUtils.repeat("x", 15486)},
                {"alwayssamekey", "2", StringUtils.repeat("y", 233)},
                {"alwayssamekey", "3", StringUtils.repeat("z", 656)}};

        final String[] fields = new String[]{
                "key text",
                "linenum int",
                "longrecord text"};

        Table dataTable = new Table("data", fields);
        dataTable.addRows(data);

        String hdfsPath = hdfsWritePath + writableTableName + "_verylongrecord";
        writableExTable.setFields(fields);
        writableExTable.setName("verylongrecordexport");
        writableExTable.setPath(hdfsPath);
        writableExTable.setFormat("CSV");
        writableExTable.setDistributionFields(new String[]{"key"});
        gpdb.createTableAndVerify(writableExTable);

        gpdb.insertData(dataTable, writableExTable);
        Assert.assertEquals("More than one segment wrote to " + hdfsPath,
                1, hdfs.list(hdfsPath).size());

        readableExTable.setFields(fields);
        readableExTable.setPath(hdfsPath);
        readableExTable.setName("verylongrecordimport");
        readableExTable.setFormat("csv");
        gpdb.createTableAndVerify(readableExTable);

        gpdb.queryResults(readableExTable,
                "SELECT * FROM " + readableExTable.getName() + " ORDER BY linenum");
        ComparisonUtils.compareTables(readableExTable, dataTable, null);
    }

    /**
     * Get HDFS path to text file (no compression or known compression), load it
     * to Table Object and compare with given data Table.
     *
     * @param hdfsPath        to text file
     * @param data            to compare to
     * @param compressionType used compression for given HDFS file
     * @throws Exception if test fails to run
     */
    private void verifyResult(String hdfsPath, Table data, EnumCompressionTypes compressionType)
            throws Exception {

        String localResultFile = dataTempFolder + "/" + hdfsPath.replaceAll("/", "_");
        // for HCFS on Cloud, wait a bit for async write in previous steps to finish
        sleep(10000);
        List<String> files = hdfs.list(hdfsPath);
        Table resultTable = new Table("result_table", null);
        int index = 0;
        for (String file : files) {
            String pathToLocalFile = localResultFile + "/_" + index;
            // make sure the file is available, saw flakes on Cloud that listed files were not available
            int attempts = 0;
            while (!hdfs.doesFileExist(file) && attempts++ < 20) {
                sleep(1000);
            }
            hdfs.copyToLocal(file, pathToLocalFile);
            sleep(250);
            resultTable.loadDataFromFile(pathToLocalFile, ",", 1, "UTF-8",
                    compressionType, true);
            index++;
        }
        // compare and ignore '\' that returns from hdfs before comma for circle types
        ComparisonUtils.compareTables(data, resultTable, null, "\\\\", "\"");
    }

    /**
     * Get HDFS path to text file (no compression), load it to Table Object and
     * compare with given data Table.
     *
     * @param hdfsPath to text file
     * @param data     to compare to
     * @throws Exception if test fails to run
     */
    private void verifyResult(String hdfsPath, Table data) throws Exception {

        verifyResult(hdfsPath, data, EnumCompressionTypes.None);
    }

    /**
     * Support all data insertion methods for writable tables.
     *
     * @param data  to insert
     * @param table to insert to
     * @throws Exception if test fails to run
     */
    private void insertData(Table data, WritableExternalTable table, InsertionMethod insertionMethod)
            throws Exception {

        switch (insertionMethod) {
            case INSERT:
                gpdb.insertData(data, table);
                break;
            case COPY:
                gpdb.copyFromStdin(data, table, ",", false);
                break;
            case INSERT_FROM_TABLE:
                gpdb.runQuery("INSERT INTO " + table.getName() + " SELECT * FROM " + data.getName());
                break;
        }
    }
}
