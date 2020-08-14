package org.greenplum.pxf.automation.testplugin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.plugins.hdfs.LineBreakAccessor;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.io.IOException;

/**
 * Test accessor, based on LineBreakAccessor.
 * This accessor throws a runtime exception after reading 10000 records.
 * Used to test an error that occurs after the first packet
 * of the response is already sent (GPSQL-2272).
 */
@Component("ThrowOn10000Accessor")
@RequestScope
public class ThrowOn10000Accessor extends LineBreakAccessor {

    private static Log Log = LogFactory.getLog(ThrowOn10000Accessor.class);
    private int rowCount;

    /**
     * Reads next record using LineBreakAccessor.readNextObject().
     * Throws a runtime exception upon reading the 10000 record.
     *
     * @return next record
     * @throws IOException if retrieving next record failed
     * @throws RuntimeException when the 10000 row is reached (expected)
     */
    @Override
    public OneRow readNextObject() throws IOException {

        OneRow oneRow = super.readNextObject();

        if ((oneRow != null) && (rowCount == 10000)) {
            throw new RuntimeException("10000 rows!");
        }
        rowCount++;

        return oneRow;
    }
}
