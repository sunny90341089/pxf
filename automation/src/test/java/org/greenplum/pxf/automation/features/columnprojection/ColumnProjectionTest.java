package org.greenplum.pxf.automation.features.columnprojection;

import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.testng.annotations.Test;

import java.io.File;

/** Functional PXF column projection cases */
public class ColumnProjectionTest extends BaseFeature {

    String testPackageLocation = "/org/greenplum/pxf/automation/testplugin/";
    String testPackage = "org.greenplum.pxf.automation.testplugin.";

    @Override
    protected void beforeClass() throws Exception {
        String newPath = "/tmp/publicstage/pxf";
        // copy additional plugins classes to cluster nodes, used for filter pushdown cases
        cluster.copyFileToNodes(new File("target/classes/" + testPackageLocation + "ColumnProjectionVerifyFragmentMetadata.class").getAbsolutePath(), newPath + testPackageLocation, true, false);
        cluster.copyFileToNodes(new File("target/classes/" + testPackageLocation + "ColumnProjectionVerifyFragmenter.class").getAbsolutePath(), newPath + testPackageLocation, true, false);
        cluster.copyFileToNodes(new File("target/classes/" + testPackageLocation + "ColumnProjectionVerifyAccessor.class").getAbsolutePath(), newPath + testPackageLocation, true, false);
        // add new path to classpath file and restart PXF service
        cluster.addPathToPxfClassPath(newPath);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
    }

    /**
     * Check PXF receive the expected column projection string from GPDB.
     * Column delimiter is ",".
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "security"})
    public void checkColumnProjection() throws Exception {

        // Create PXF external table for column projection testing
        ReadableExternalTable pxfExternalTable = new ReadableExternalTable("test_column_projection", new String[] {
                "t0    text",
                "a1    integer",
                "b2    boolean",
                "colprojValue  text"
        }, "dummy_path","TEXT");

        pxfExternalTable.setFragmenter(testPackage + "ColumnProjectionVerifyFragmenter");
        pxfExternalTable.setAccessor(testPackage + "ColumnProjectionVerifyAccessor");
        pxfExternalTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        pxfExternalTable.setDelimiter(",");
        pxfExternalTable.setHost(pxfHost);
        pxfExternalTable.setPort(pxfPort);

        gpdb.createTableAndVerify(pxfExternalTable);

        runTincTest("pxf.features.columnprojection.checkColumnProjection.runTest");
    }
}
