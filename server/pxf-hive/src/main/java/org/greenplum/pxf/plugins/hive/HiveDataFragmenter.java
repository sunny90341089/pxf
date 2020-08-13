package org.greenplum.pxf.plugins.hive;

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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.api.model.Metadata;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.greenplum.pxf.plugins.hive.utilities.ProfileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.greenplum.pxf.api.model.Fragment.HOSTS;

/**
 * Fragmenter class for HIVE tables. <br>
 * Given a Hive table and its partitions divide the data into fragments (here a
 * data fragment is actually a HDFS file block) and return a list of them. Each
 * data fragment will contain the following information:
 * <ol>
 * <li>sourceName: full HDFS path to the data file that this data fragment is
 * part of</li>
 * <li>hosts: a list of the datanode machines that hold a replica of this block</li>
 * <li>userData:
 * file_input_format_name_DELIM_serde_name_DELIM_serialization_properties</li>
 * </ol>
 */
@Component("HiveDataFragmenter")
@RequestScope
public class HiveDataFragmenter extends HdfsDataFragmenter {
    private static final Logger LOG = LoggerFactory.getLogger(HiveDataFragmenter.class);
    private static final short ALL_PARTS = -1;

    public static final String HIVE_1_PART_DELIM = "!H1PD!";
    public static final String HIVE_PARTITIONS_DELIM = "!HPAD!";
    public static final String HIVE_NO_PART_TBL = "!HNPT!";

    static final EnumSet<Operator> SUPPORTED_OPERATORS =
            EnumSet.of(
                    Operator.EQUALS,
                    Operator.LESS_THAN,
                    Operator.GREATER_THAN,
                    Operator.LESS_THAN_OR_EQUAL,
                    Operator.GREATER_THAN_OR_EQUAL,
                    Operator.NOT_EQUALS,
                    Operator.AND,
                    Operator.OR
            );

    private static final TreeTraverser TRAVERSER = new TreeTraverser();

    private IMetaStoreClient client;
    private HiveClientWrapper hiveClientWrapper;
    protected HiveUtilities hiveUtilities;

    private boolean filterInFragmenter = false;

    // Data structure to hold hive partition names if exist, to be used by
    // partition filtering
    private final Set<String> setPartitions = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private final Map<String, String> partitionKeyTypes = new HashMap<>();

    /**
     * Sets the {@link HiveClientWrapper} object
     *
     * @param hiveClientWrapper the hive client wrapper object
     */
    @Autowired
    public void setHiveClientWrapper(HiveClientWrapper hiveClientWrapper) {
        this.hiveClientWrapper = hiveClientWrapper;
    }

    /**
     * Sets the {@link HiveUtilities} object
     *
     * @param hiveUtilities the hive utilities object
     */
    @Autowired
    public void setHiveUtilities(HiveUtilities hiveUtilities) {
        this.hiveUtilities = hiveUtilities;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        client = hiveClientWrapper.initHiveClient(this.context, configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        Metadata.Item tblDesc = hiveClientWrapper.extractTableFromName(context.getDataSource());
        fetchTableMetaData(tblDesc);
        return fragments;
    }

    /*
     * Goes over the table partitions metadata and extracts the splits and the
     * InputFormat and Serde per split.
     */
    private void fetchTableMetaData(Metadata.Item tblDesc) throws Exception {

        Table tbl = hiveClientWrapper.getHiveTable(client, tblDesc);

        Metadata metadata = new Metadata(tblDesc);
        hiveClientWrapper.getSchema(tbl, metadata);
        boolean hasComplexTypes = hiveClientWrapper.hasComplexTypes(metadata);

        // Keep a list of indices from the Hive schema columns that we need to
        // retrieve
        List<Integer> hiveIndexes = verifySchema(tbl);
        List<FieldSchema> fieldSchemaList = tbl.getSd().getCols();

        // Get the column names and column types
        StringBuilder allColumnNames = new StringBuilder();
        StringBuilder allColumnTypes = new StringBuilder();
        String delim = ",";
        for (FieldSchema fieldSchema : fieldSchemaList) {
            if (allColumnNames.length() > 0) {
                allColumnNames.append(delim);
                allColumnTypes.append(delim);
            }
            allColumnNames.append(fieldSchema.getName());
            allColumnTypes.append(fieldSchema.getType());
        }

        List<Partition> partitions;
        String filterStringForHive = "";

        // If query has filter and hive table has partitions, prepare the filter
        // string for hive metastore and retrieve only the matched partitions
        if (context.hasFilter() && tbl.getPartitionKeysSize() > 0) {

            // Save all hive partition names in a set for later filter match
            for (FieldSchema fs : tbl.getPartitionKeys()) {
                setPartitions.add(fs.getName());
                partitionKeyTypes.put(fs.getName(), fs.getType());
            }

            LOG.debug("setPartitions: {}", setPartitions);

            // canPushDownIntegral represents hive.metastore.integral.jdo.pushdown property in hive-site.xml
            boolean canPushDownIntegral = configuration
                    .getBoolean(HiveConf.ConfVars.METASTORE_INTEGER_JDO_PUSHDOWN.varname, false);

            List<ColumnDescriptor> columnDescriptors = context.getTupleDescription();

            HivePartitionFilterBuilder hivePartitionFilterBuilder = new HivePartitionFilterBuilder(columnDescriptors);
            TreeVisitor hivePartitionPruner = new HivePartitionPruner(SUPPORTED_OPERATORS,
                    canPushDownIntegral, partitionKeyTypes, columnDescriptors);

            // Parse the filter string into a expression tree Node
            Node root = new FilterParser().parse(context.getFilterString());
            // Prune the parsed tree with valid supported operators and then
            // traverse the pruned tree with the hivePartitionFilterBuilder to produce a filter string for hive
            TRAVERSER.traverse(root, hivePartitionPruner, hivePartitionFilterBuilder);

            // Generate filter string for retrieve match pxf filter/hive partition name
            filterStringForHive = hivePartitionFilterBuilder.toString();
        }

        if (StringUtils.isNotBlank(filterStringForHive)) {

            LOG.debug("Filter String for Hive partition retrieval : {}",
                    filterStringForHive);

            filterInFragmenter = true;

            // API call to Hive MetaStore, will return a List of all the
            // partitions for this table, that matches the partition filters
            // Defined in filterStringForHive.
            partitions = client.listPartitionsByFilter(tblDesc.getPath(),
                    tblDesc.getName(), filterStringForHive, ALL_PARTS);

            // No matched partitions for the filter, no fragments to return.
            if (partitions == null || partitions.isEmpty()) {

                LOG.debug("Table - {}.{} has no matched partitions for the filter : {}",
                        tblDesc.getPath(), tblDesc.getName(), filterStringForHive);
                return;
            }

            LOG.debug("Table - {}.{} matched partitions list size: {}",
                    tblDesc.getPath(), tblDesc.getName(), partitions.size());

        } else {
            // API call to Hive MetaStore, will return a List of all the
            // partitions for this table (no filtering)
            partitions = client.listPartitions(tblDesc.getPath(),
                    tblDesc.getName(), ALL_PARTS);
        }

        StorageDescriptor descTable = tbl.getSd();
        Properties props;

        if (partitions.isEmpty()) {
            props = getSchema(tbl);
            fetchMetaDataForSimpleTable(descTable, props, hasComplexTypes,
                    hiveIndexes, allColumnNames.toString(), allColumnTypes.toString());
        } else {
            List<FieldSchema> partitionKeys = tbl.getPartitionKeys();

            for (Partition partition : partitions) {
                StorageDescriptor descPartition = partition.getSd();
                props = MetaStoreUtils.getSchema(descPartition, descTable,
                        null,
                        tblDesc.getPath(), tblDesc.getName(),
                        partitionKeys);
                fetchMetaDataForPartitionedTable(descPartition, props, partition,
                        partitionKeys, tblDesc.getName(), hasComplexTypes,
                        hiveIndexes, allColumnNames.toString(), allColumnTypes.toString());
            }
        }
    }

    /**
     * Verifies that all the Greenplum defined columns are present in the Hive
     * table schema. Then return a list of indexes corresponding to the
     * matching columns in Greenplum, ordered by the Greenplum schema order.
     *
     * @param tbl the hive table
     * @return a list of indexes
     */
    List<Integer> verifySchema(Table tbl) {

        List<Integer> indexes = new ArrayList<>();
        List<FieldSchema> hiveColumns = tbl.getSd().getCols();
        List<FieldSchema> hivePartitions = tbl.getPartitionKeys();

        Set<String> columnAndPartitionNames =
                Stream.concat(hiveColumns.stream(), hivePartitions.stream())
                        .map(FieldSchema::getName)
                        .collect(Collectors.toSet());

        Map<String, Integer> columnNameToColsIndexMap =
                IntStream.range(0, hiveColumns.size())
                        .boxed()
                        .collect(Collectors.toMap(i -> hiveColumns.get(i).getName(), i -> i));

        for (ColumnDescriptor cd : context.getTupleDescription()) {
            if (!columnAndPartitionNames.contains(cd.columnName()) &&
                    !columnAndPartitionNames.contains(cd.columnName().toLowerCase())) {
                throw new PxfRuntimeException(
                        String.format("column '%s' does not exist in the Hive schema", cd.columnName()),
                        "Ensure the column exists and check the column name spelling and case."
                );
            }

            // The index of the column on the Hive schema
            Integer index =
                    defaultIfNull(columnNameToColsIndexMap.get(cd.columnName()),
                            columnNameToColsIndexMap.get(cd.columnName().toLowerCase()));
            indexes.add(index);
        }
        return indexes;
    }

    private static Properties getSchema(Table table) {
        return MetaStoreUtils.getSchema(table.getSd(), table.getSd(),
                table.getParameters(), table.getDbName(), table.getTableName(),
                table.getPartitionKeys());
    }

    private void fetchMetaDataForSimpleTable(StorageDescriptor stdsc,
                                             Properties props,
                                             boolean hasComplexTypes,
                                             List<Integer> hiveIndexes,
                                             String allColumnNames,
                                             String allColumnTypes) throws Exception {
        fetchMetaDataForSimpleTable(stdsc, props, null, hasComplexTypes,
                hiveIndexes, allColumnNames, allColumnTypes);
    }

    private void fetchMetaDataForSimpleTable(StorageDescriptor stdsc,
                                             Properties props,
                                             String tableName,
                                             boolean hasComplexTypes,
                                             List<Integer> hiveIndexes,
                                             String allColumnNames,
                                             String allColumnTypes)
            throws Exception {
        fetchMetaData(new HiveTablePartition(stdsc, props, null, null,
                tableName), hasComplexTypes, hiveIndexes, allColumnNames, allColumnTypes);
    }

    private void fetchMetaDataForPartitionedTable(StorageDescriptor stdsc,
                                                  Properties props,
                                                  Partition partition,
                                                  List<FieldSchema> partitionKeys,
                                                  String tableName,
                                                  boolean hasComplexTypes,
                                                  List<Integer> hiveIndexes,
                                                  String allColumnNames,
                                                  String allColumnTypes)
            throws Exception {
        fetchMetaData(new HiveTablePartition(stdsc, props, partition,
                        partitionKeys, tableName), hasComplexTypes, hiveIndexes,
                allColumnNames, allColumnTypes);
    }

    /* Fills a table partition */
    private void fetchMetaData(HiveTablePartition tablePartition,
                               boolean hasComplexTypes,
                               List<Integer> hiveIndexes,
                               String allColumnNames,
                               String allColumnTypes)
            throws Exception {
        JobConf jobConf = getJobConf();
        InputFormat<?, ?> fformat = hiveUtilities.makeInputFormat(tablePartition.storageDesc.getInputFormat(), jobConf);
        String profile = null;
        String userProfile = context.getProfile();
        if (userProfile != null) {
            // evaluate optimal profile based on file format if profile was explicitly specified in url
            // if user passed accessor+fragmenter+resolver - use them
            profile = ProfileFactory.get(fformat, hasComplexTypes, userProfile);
        }
        String fragmenterForProfile;
        if (profile != null) {
            fragmenterForProfile = context.getPluginConf().getPlugins(profile).get("FRAGMENTER");
        } else {
            fragmenterForProfile = context.getFragmenter();
        }

        FileInputFormat.setInputPaths(jobConf, new Path(
                tablePartition.storageDesc.getLocation()));

        InputSplit[] splits;
        try {
            splits = fformat.getSplits(jobConf, 1);
        } catch (org.apache.hadoop.mapred.InvalidInputException e) {
            LOG.debug("getSplits failed on " + e.getMessage());
            return;
        }

        for (InputSplit split : splits) {
            FileSplit fsp = (FileSplit) split;
            String filepath = fsp.getPath().toString();

            HiveFragmentMetadata.Builder builder = hiveClientWrapper
                    .buildMetadata(fragmenterForProfile, tablePartition)
                    .withFileSplit(fsp)
                    .withHiveIndexes(hiveIndexes)
                    .withAllColumnNames(allColumnNames)
                    .withAllColumnTypes(allColumnTypes)
                    .withFilterInFragmenter(filterInFragmenter);

            Fragment fragment = new Fragment(filepath, HOSTS, builder.build(), profile);
            fragments.add(fragment);
        }
    }

    /**
     * Returns statistics for Hive table. Currently it's not implemented.
     */
    @Override
    public FragmentStats getFragmentStats() throws Exception {
        Metadata.Item tblDesc = hiveClientWrapper.extractTableFromName(context.getDataSource());
        Table tbl = hiveClientWrapper.getHiveTable(client, tblDesc);
        Metadata metadata = new Metadata(tblDesc);
        hiveClientWrapper.getSchema(tbl, metadata);

        long split_count = Long.parseLong(tbl.getParameters().get("numFiles"));
        long totalSize = Long.parseLong(tbl.getParameters().get("totalSize"));
        long firstFragmentSize = totalSize / split_count;
        return new FragmentStats(split_count, firstFragmentSize, totalSize);
    }

}
