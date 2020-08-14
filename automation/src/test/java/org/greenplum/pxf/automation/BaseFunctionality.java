package org.greenplum.pxf.automation;

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.automation.structures.tables.basic.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Functionality Tests Base Class
 */
public abstract class BaseFunctionality extends BaseTestParent {
    // file name for storing data on HDFS
    protected String fileName = "data.txt";

    /**
     * Create Data Table with small data
     * with numRows number of rows
     * following fields: int, String, double, long and boolean
     *
     * @return Table
     * @throws IOException
     */
    protected Table getSmallData(String uniqueName, int numRows) {
        List<List<String>> data = new ArrayList<>();

        for (int i = 1; i <= numRows; i++) {
            List<String> row = new ArrayList<>();
            row.add(String.format("%s%srow_%d", uniqueName, StringUtils.isBlank(uniqueName) ? "" : "_", i));
            row.add(String.valueOf(i));
            row.add(String.valueOf(Double.toString(i)));
            row.add(Long.toString(100000000000L * i));
            row.add(String.valueOf(i % 2 == 0));
            data.add(row);
        }

        Table dataTable = new Table("dataTable", null);
        dataTable.setData(data);

        return dataTable;
    }

    protected Table getSmallData() throws IOException {
        return getSmallData("");
    }

    protected Table getSmallData(String uniqueName) {
        return getSmallData(uniqueName, 100);
    }

    @Override
    protected void runTincTest(String tincTest) throws Exception {
        try {
            if (!tincTest.contains("hcatalog")) {
                super.runTincTest(tincTest);
            }  // else These features/test cases are not supported. Do Nothing.
        } catch (Exception e) {
            throw new Exception("Tinc Failure (" + e.getMessage() + ")");
        }
    }
}
