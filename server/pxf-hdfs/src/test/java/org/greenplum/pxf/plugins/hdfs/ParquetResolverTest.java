package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.pig.convert.DecimalUtils;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetTypeConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParquetResolverTest {

    private ParquetResolver resolver;
    private RequestContext context;
    private MessageType schema;
    private String localTimestampString;

    @BeforeEach
    public void setup() {
        resolver = new ParquetResolver();
        context = new RequestContext();
        schema = new MessageType("test");
        context.setMetadata(schema);
        context.setConfig("default");
        context.setUser("test-user");

        // for test cases that test conversions against server's time zone
        Instant timestamp = Instant.parse("2013-07-14T04:00:05Z"); // UTC
        ZonedDateTime localTime = timestamp.atZone(ZoneId.systemDefault());
        localTimestampString = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")); // should be "2013-07-13 21:00:05" in PST

    }

    @Test
    public void testInitialize() {
        resolver.setRequestContext(context);
    }

    @Test
    public void testGetFields_FailsOnMissingSchema() {
        context.setMetadata(null);
        resolver.setRequestContext(context);

        Exception e = assertThrows(RuntimeException.class,
                () -> resolver.getFields(new OneRow()));
        assertEquals("No schema detected in request context", e.getMessage());
    }

    @Test
    public void testSetFields_FailsOnMissingSchema() {
        context.setMetadata(null);
        resolver.setRequestContext(context);

        Exception e = assertThrows(RuntimeException.class,
                () -> resolver.setFields(new ArrayList<>()));
        assertEquals("No schema detected in request context", e.getMessage());
    }

    @Test
    public void testSetFields_Primitive() throws IOException {
        schema = getParquetSchemaForPrimitiveTypes(Type.Repetition.OPTIONAL, false);
        // schema has changed, set metadata again
        context.setMetadata(schema);
        resolver.setRequestContext(context);
        context.setTupleDescription(getColumnDescriptorsFromSchema(schema));

        Instant timestamp = Instant.parse("2013-07-14T04:00:05Z"); // UTC
        ZonedDateTime localTime = timestamp.atZone(ZoneId.systemDefault());
        String localTimestampString = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")); // should be "2013-07-13 21:00:05" in PST

        List<OneField> fields = new ArrayList<>();
        fields.add(new OneField(DataType.TEXT.getOID(), "row1"));
        fields.add(new OneField(DataType.TEXT.getOID(), "s_6"));
        fields.add(new OneField(DataType.INTEGER.getOID(), 1));
        fields.add(new OneField(DataType.FLOAT8.getOID(), 6.0d));
        fields.add(new OneField(DataType.NUMERIC.getOID(), "1.234560000000000000"));
        fields.add(new OneField(DataType.TIMESTAMP.getOID(), localTimestampString));
        fields.add(new OneField(DataType.REAL.getOID(), 7.7f));
        fields.add(new OneField(DataType.BIGINT.getOID(), 23456789L));
        fields.add(new OneField(DataType.BOOLEAN.getOID(), false));
        fields.add(new OneField(DataType.SMALLINT.getOID(), (short) 1));
        fields.add(new OneField(DataType.SMALLINT.getOID(), (short) 10));
        fields.add(new OneField(DataType.TEXT.getOID(), "abcd"));
        fields.add(new OneField(DataType.TEXT.getOID(), "abc"));
        fields.add(new OneField(DataType.BYTEA.getOID(), new byte[]{(byte) 49}));
        fields.add(new OneField(DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), "2013-07-13 21:00:05-07"));
        fields.add(new OneField(DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), "2013-07-14 16:45:05+12:45"));
        OneRow row = resolver.setFields(fields);
        assertNotNull(row);
        Object data = row.getData();
        assertNotNull(data);
        assertTrue(data instanceof Group);
        Group group = (Group) data;

        // assert column values
        assertEquals("row1", group.getString(0, 0));
        assertEquals("s_6", group.getString(1, 0));
        assertEquals(1, group.getInteger(2, 0));
        assertEquals(6.0d, group.getDouble(3, 0), 0d);
        assertEquals(BigDecimal.valueOf(1234560000000000000L, 18),
                DecimalUtils.binaryToDecimal(group.getBinary(4, 0), 19, 18));

        NanoTime nanoTime = NanoTime.fromBinary(group.getInt96(5, 0));
        assertEquals(2456488, nanoTime.getJulianDay()); // 14 Jul 2013 in Julian days
        assertEquals((4 * 60 * 60 + 5L) * 1000 * 1000 * 1000, nanoTime.getTimeOfDayNanos()); // 04:00:05 time
        assertEquals(7.7f, group.getFloat(6, 0), 0f);
        assertEquals(23456789L, group.getLong(7, 0));
        assertFalse(group.getBoolean(8, 0));
        assertEquals(1, group.getInteger(9, 0));
        assertEquals(10, group.getInteger(10, 0));
        assertEquals("abcd", group.getString(11, 0));
        assertEquals("abc", group.getString(12, 0));
        assertArrayEquals(new byte[]{(byte) 49}, group.getBinary(13, 0).getBytes());

        nanoTime = NanoTime.fromBinary(group.getInt96(14, 0));
        assertEquals(2456488, nanoTime.getJulianDay()); // 14 Jul 2013 in Julian days
        assertEquals((4 * 60 * 60 + 5L) * 1000 * 1000 * 1000, nanoTime.getTimeOfDayNanos()); // 04:00:05 time

        nanoTime = NanoTime.fromBinary(group.getInt96(15, 0));
        assertEquals(2456488, nanoTime.getJulianDay()); // 14 Jul 2013 in Julian days
        assertEquals((4 * 60 * 60 + 5L) * 1000 * 1000 * 1000, nanoTime.getTimeOfDayNanos()); // 04:00:05 time

        // assert value repetition count
        for (int i = 0; i < 16; i++) {
            assertEquals(1, group.getFieldRepetitionCount(i));
        }
    }

    @Test
    public void testSetFields_RightTrimChar() throws IOException {
        testSetFields_RightTrimCharHelper("abcd  ", "abc   ", "abc");
    }

    @Test
    public void testSetFields_RightTrimCharDontTrimTabChars() throws IOException {
        testSetFields_RightTrimCharHelper("abcd\t\t", "abc\t\t\t ", "abc\t\t\t");
    }

    @Test
    public void testSetFields_RightTrimCharDontTrimNewlineChars() throws IOException {
        testSetFields_RightTrimCharHelper("abcd\n\n", "abc\n\n\n ", "abc\n\n\n");
    }

    @Test
    public void testSetFields_RightTrimCharNoLeftTrim() throws IOException {
        testSetFields_RightTrimCharHelper("  abcd  ", "  abc   ", "  abc");
    }

    @Test
    public void testSetFields_Primitive_Nulls() throws IOException {
        schema = getParquetSchemaForPrimitiveTypes(Type.Repetition.OPTIONAL, false);
        // schema has changed, set metadata again
        context.setMetadata(schema);
        context.setTupleDescription(getColumnDescriptorsFromSchema(schema));
        resolver.setRequestContext(context);
        List<OneField> fields = new ArrayList<>();
        fields.add(new OneField(DataType.TEXT.getOID(), null));
        fields.add(new OneField(DataType.TEXT.getOID(), null));
        fields.add(new OneField(DataType.INTEGER.getOID(), null));
        fields.add(new OneField(DataType.FLOAT8.getOID(), null));
        fields.add(new OneField(DataType.NUMERIC.getOID(), null));
        fields.add(new OneField(DataType.TIMESTAMP.getOID(), null));
        fields.add(new OneField(DataType.REAL.getOID(), null));
        fields.add(new OneField(DataType.BIGINT.getOID(), null));
        fields.add(new OneField(DataType.BOOLEAN.getOID(), null));
        fields.add(new OneField(DataType.SMALLINT.getOID(), null));
        fields.add(new OneField(DataType.SMALLINT.getOID(), null));
        fields.add(new OneField(DataType.TEXT.getOID(), null));
        fields.add(new OneField(DataType.TEXT.getOID(), null));
        fields.add(new OneField(DataType.BYTEA.getOID(), null));
        fields.add(new OneField(DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), null));
        fields.add(new OneField(DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), null));


        OneRow row = resolver.setFields(fields);
        assertNotNull(row);
        Object data = row.getData();
        assertNotNull(data);
        assertTrue(data instanceof Group);
        Group group = (Group) data;
        // assert value repetition count
        for (int i = 0; i < 16; i++) {
            assertEquals(0, group.getFieldRepetitionCount(i));
        }
    }

    @Test
    public void testGetFields_Primitive_EmptySchema() throws IOException {
        resolver.setRequestContext(context);

        List<Group> groups = readParquetFile("primitive_types.parquet", 25, schema);
        OneRow row1 = new OneRow(groups.get(0)); // get row 1
        List<OneField> fields = resolver.getFields(row1);
        assertTrue(fields.isEmpty());
    }

    @Test
    public void testGetFields_Primitive() throws IOException {
        schema = getParquetSchemaForPrimitiveTypes(Type.Repetition.OPTIONAL, true);
        // schema has changed, set metadata again
        context.setMetadata(schema);
        context.setTupleDescription(getColumnDescriptorsFromSchema(schema));
        resolver.setRequestContext(context);

        List<Group> groups = readParquetFile("primitive_types.parquet", 25, schema);
        assertEquals(25, groups.size());

        List<OneField> fields = assertRow(groups, 0, 16);
        //s1 : "row1" : TEXT
        assertField(fields, 0, "row1", DataType.TEXT);
        assertField(fields, 1, "s_6", DataType.TEXT);
        assertField(fields, 2, 1, DataType.INTEGER);
        assertField(fields, 3, 6.0d, DataType.FLOAT8);
        assertField(fields, 4, BigDecimal.valueOf(1234560000000000000L, 18), DataType.NUMERIC);
        assertField(fields, 5, localTimestampString, DataType.TIMESTAMP);
        assertField(fields, 6, 7.7f, DataType.REAL);
        assertField(fields, 7, 23456789L, DataType.BIGINT);
        assertField(fields, 8, false, DataType.BOOLEAN);
        assertField(fields, 9, (short) 1, DataType.SMALLINT);
        assertField(fields, 10, (short) 10, DataType.SMALLINT);
        assertField(fields, 11, "abcd", DataType.TEXT);
        assertField(fields, 12, "abc", DataType.TEXT);
        assertField(fields, 13, new byte[]{(byte) 49}, DataType.BYTEA); // 49 is the ascii code for '1'
        // Parquet only stores the Timestamp (timezone information was lost)
        assertField(fields, 14, localTimestampString, DataType.TIMESTAMP);
        // Parquet only stores the Timestamp (timezone information was lost)
        assertField(fields, 15, localTimestampString, DataType.TIMESTAMP);

        // test nulls
        fields = assertRow(groups, 11, 16);
        assertField(fields, 1, null, DataType.TEXT);
        fields = assertRow(groups, 12, 16);
        assertField(fields, 2, null, DataType.INTEGER);
        fields = assertRow(groups, 13, 16);
        assertField(fields, 3, null, DataType.FLOAT8);
        fields = assertRow(groups, 14, 16);
        assertField(fields, 4, null, DataType.NUMERIC);
        fields = assertRow(groups, 15, 16);
        assertField(fields, 5, null, DataType.TIMESTAMP);
        fields = assertRow(groups, 16, 16);
        assertField(fields, 6, null, DataType.REAL);
        fields = assertRow(groups, 17, 16);
        assertField(fields, 7, null, DataType.BIGINT);
        fields = assertRow(groups, 18, 16);
        assertField(fields, 8, null, DataType.BOOLEAN);
        fields = assertRow(groups, 19, 16);
        assertField(fields, 9, null, DataType.SMALLINT);
        fields = assertRow(groups, 20, 16);
        assertField(fields, 10, null, DataType.SMALLINT);
        fields = assertRow(groups, 22, 16);
        assertField(fields, 11, null, DataType.TEXT);
        fields = assertRow(groups, 23, 16);
        assertField(fields, 12, null, DataType.TEXT);
        fields = assertRow(groups, 24, 16);
        assertField(fields, 13, null, DataType.BYTEA);
    }

    @Test
    public void testGetFields_Primitive_With_Projection() throws IOException {
        schema = getParquetSchemaForPrimitiveTypes(Type.Repetition.OPTIONAL, true);
        context.setTupleDescription(getColumnDescriptorsFromSchema(schema));

        // set odd columns to be not projected, their values will become null
        for (int i = 0; i < context.getTupleDescription().size(); i++) {
            context.getTupleDescription().get(i).setProjected(i % 2 == 0);
        }

        MessageType readSchema = buildReadSchema(schema);
        // schema has changed, set metadata again
        context.setMetadata(readSchema);

        resolver.setRequestContext(context);

        // use readSchema to read only specific columns from parquet file into Group
        List<Group> groups = readParquetFile("primitive_types.parquet", 25, readSchema);
        assertEquals(25, groups.size());

        List<OneField> fields = assertRow(groups, 0, 16);
        //s1 : "row1" : TEXT
        assertField(fields, 0, "row1", DataType.TEXT);
        assertField(fields, 1, null, DataType.TEXT);
        assertField(fields, 2, 1, DataType.INTEGER);
        assertField(fields, 3, null, DataType.FLOAT8);
        assertField(fields, 4, BigDecimal.valueOf(1234560000000000000L, 18), DataType.NUMERIC);
        assertField(fields, 5, null, DataType.TIMESTAMP);
        assertField(fields, 6, 7.7f, DataType.REAL);
        assertField(fields, 7, null, DataType.BIGINT);
        assertField(fields, 8, false, DataType.BOOLEAN);
        assertField(fields, 9, null, DataType.SMALLINT);
        assertField(fields, 10, (short) 10, DataType.SMALLINT);
        assertField(fields, 11, null, DataType.TEXT);
        assertField(fields, 12, "abc", DataType.TEXT);
        assertField(fields, 13, null, DataType.BYTEA); // 49 is the ascii code for '1'
        // Parquet only stores the Timestamp (timezone information was lost)
        assertField(fields, 14, localTimestampString, DataType.TIMESTAMP);
        // Parquet only stores the Timestamp (timezone information was lost)
        assertField(fields, 15, null, DataType.TIMESTAMP);

        // test nulls
        fields = assertRow(groups, 11, 16);
        assertField(fields, 1, null, DataType.TEXT);
        fields = assertRow(groups, 12, 16);
        assertField(fields, 2, null, DataType.INTEGER);
        fields = assertRow(groups, 13, 16);
        assertField(fields, 3, null, DataType.FLOAT8);
        fields = assertRow(groups, 14, 16);
        assertField(fields, 4, null, DataType.NUMERIC);
        fields = assertRow(groups, 15, 16);
        assertField(fields, 5, null, DataType.TIMESTAMP);
        fields = assertRow(groups, 16, 16);
        assertField(fields, 6, null, DataType.REAL);
        fields = assertRow(groups, 17, 16);
        assertField(fields, 7, null, DataType.BIGINT);
        fields = assertRow(groups, 18, 16);
        assertField(fields, 8, null, DataType.BOOLEAN);
        fields = assertRow(groups, 19, 16);
        assertField(fields, 9, null, DataType.SMALLINT);
        fields = assertRow(groups, 20, 16);
        assertField(fields, 10, null, DataType.SMALLINT);
        fields = assertRow(groups, 22, 16);
        assertField(fields, 11, null, DataType.TEXT);
        fields = assertRow(groups, 23, 16);
        assertField(fields, 12, null, DataType.TEXT);
        fields = assertRow(groups, 24, 16);
        assertField(fields, 13, null, DataType.BYTEA);
    }

    @Test
    public void testGetFields_Primitive_RepeatedString() throws IOException {
        List<Type> columns = new ArrayList<>();
        columns.add(new PrimitiveType(Type.Repetition.REPEATED, PrimitiveTypeName.BINARY, "myString", OriginalType.UTF8));
        schema = new MessageType("TestProtobuf.StringArray", columns);
        context.setMetadata(schema);
        context.setTupleDescription(getColumnDescriptorsFromSchema(schema));
        resolver.setRequestContext(context);

        List<Group> groups = readParquetFile("proto-repeated-string.parquet", 3, schema);
        List<OneField> fields;

        // row 0
        fields = assertRow(groups, 0, 1);
        assertEquals(DataType.TEXT.getOID(), fields.get(0).type);
        assertEquals("[\"hello\",\"world\"]", fields.get(0).val);

        // row 1
        fields = assertRow(groups, 1, 1);
        assertEquals(DataType.TEXT.getOID(), fields.get(0).type);
        assertEquals("[\"good\",\"bye\"]", fields.get(0).val);

        // row 2
        fields = assertRow(groups, 2, 1);
        assertEquals(DataType.TEXT.getOID(), fields.get(0).type);
        assertEquals("[\"one\",\"two\",\"three\"]", fields.get(0).val);

    }

    @Test
    public void testGetFields_Primitive_Repeated_Synthetic() {
        // this test does not read the actual Parquet file, but rather construct Group object synthetically
        schema = getParquetSchemaForPrimitiveTypes(Type.Repetition.REPEATED, true);
        // schema has changed, set metadata again
        context.setMetadata(schema);
        context.setTupleDescription(getColumnDescriptorsFromSchema(schema));
        resolver.setRequestContext(context);

        /*
        Corresponding DB column types  are:
        TEXT,TEXT,INTEGER, DOUBLE PRECISION,NUMERIC,TIMESTAMP,REAL,BIGINT,BOOLEAN,SMALLINT,SMALLINT,VARCHAR(5),CHAR(3),BYTEA
         */

        Group group = new SimpleGroup(schema);

        group.add(0, "row1-1");
        group.add(0, "row1-2");

        // leave column 1 (t2) unset as part fo the test

        group.add(2, 1);
        group.add(2, 2);
        group.add(2, 3);

        group.add(3, 6.0d);
        group.add(3, -16.34d);

        BigDecimal value = new BigDecimal("12345678.9012345987654321"); // place of dot doesn't matter
        byte fillByte = (byte) (value.signum() < 0 ? 0xFF : 0x00);
        byte[] unscaled = value.unscaledValue().toByteArray();
        byte[] bytes = new byte[16];
        int offset = bytes.length - unscaled.length;
        for (int i = 0; i < bytes.length; i += 1) {
            bytes[i] = (i < offset) ? fillByte : unscaled[i - offset];
        }
        group.add(4, Binary.fromReusedByteArray(bytes));

        group.add(5, ParquetTypeConverter.getBinaryFromTimestamp("2019-03-14 14:10:28"));
        group.add(5, ParquetTypeConverter.getBinaryFromTimestamp("1969-12-30 05:42:23.211211"));

        group.add(6, 7.7f);
        group.add(6, -12345.35354646f);

        group.add(7, 23456789L);
        group.add(7, -123456789012345L);

        group.add(8, true);
        group.add(8, false);

        group.add(9, (short) 1);
        group.add(9, (short) -3);

        group.add(10, (short) 269);
        group.add(10, (short) -313);

        group.add(11, Binary.fromString("Hello"));
        group.add(11, Binary.fromString("World"));

        group.add(12, Binary.fromString("foo"));
        group.add(12, Binary.fromString("bar"));

        byte[] byteArray1 = new byte[]{(byte) 49, (byte) 50, (byte) 51};
        group.add(13, Binary.fromReusedByteArray(byteArray1, 0, 3));
        byte[] byteArray2 = new byte[]{(byte) 52, (byte) 53, (byte) 54};
        group.add(13, Binary.fromReusedByteArray(byteArray2, 0, 3));

        group.add(14, ParquetTypeConverter.getBinaryFromTimestampWithTimeZone("2019-03-14 14:10:28+07"));
        OffsetDateTime offsetDateTime1 = OffsetDateTime.parse("2019-03-14T14:10:28+07:00");
        ZonedDateTime localDateTime1 = offsetDateTime1.atZoneSameInstant(ZoneId.systemDefault());
        String localDateTimeString1 = localDateTime1.format(DateTimeFormatter.ofPattern("[yyyy-MM-dd HH:mm:ss]"));

        group.add(15, ParquetTypeConverter.getBinaryFromTimestampWithTimeZone("2019-03-14 14:10:28-07:30"));
        OffsetDateTime offsetDateTime2 = OffsetDateTime.parse("2019-03-14T14:10:28-07:30");
        ZonedDateTime localDateTime2 = offsetDateTime2.atZoneSameInstant(ZoneId.systemDefault());
        String localDateTimeString2 = localDateTime2.format(DateTimeFormatter.ofPattern("[yyyy-MM-dd HH:mm:ss]"));


        List<Group> groups = new ArrayList<>();
        groups.add(group);
        List<OneField> fields = assertRow(groups, 0, 16);

        assertField(fields, 0, "[\"row1-1\",\"row1-2\"]", DataType.TEXT);
        assertField(fields, 1, "[]", DataType.TEXT);
        assertField(fields, 2, "[1,2,3]", DataType.TEXT);
        assertField(fields, 3, "[6.0,-16.34]", DataType.TEXT);
        assertField(fields, 4, "[123456.789012345987654321]", DataType.TEXT); // scale fixed to 18 in schema
        assertField(fields, 5, "[\"2019-03-14 14:10:28\",\"1969-12-30 05:42:23.211211\"]", DataType.TEXT);
        assertField(fields, 6, "[7.7,-12345.354]", DataType.TEXT); // rounded to the precision of 8
        assertField(fields, 7, "[23456789,-123456789012345]", DataType.TEXT);
        assertField(fields, 8, "[true,false]", DataType.TEXT);
        assertField(fields, 9, "[1,-3]", DataType.TEXT);
        assertField(fields, 10, "[269,-313]", DataType.TEXT);
        assertField(fields, 11, "[\"Hello\",\"World\"]", DataType.TEXT);
        assertField(fields, 12, "[\"foo\",\"bar\"]", DataType.TEXT); // 3 chars only
        Base64.Encoder encoder = Base64.getEncoder(); // byte arrays are Base64 encoded into strings
        String expectedByteArrays = "[\"" + encoder.encodeToString(byteArray1) + "\",\"" + encoder.encodeToString(byteArray2) + "\"]";
        assertField(fields, 13, expectedByteArrays, DataType.TEXT);
        assertField(fields, 14, "[\"" + localDateTimeString1 + "\"]", DataType.TEXT);
        assertField(fields, 15, "[\"" + localDateTimeString2 + "\"]", DataType.TEXT);
    }

    @Test
    public void testGetFields_Primitive_RepeatedInt() throws IOException {
        List<Type> columns = new ArrayList<>();
        columns.add(new PrimitiveType(Type.Repetition.REPEATED, PrimitiveTypeName.INT32, "repeatedInt"));
        schema = new MessageType("TestProtobuf.RepeatedIntMessage", columns);
        context.setMetadata(schema);
        context.setTupleDescription(getColumnDescriptorsFromSchema(schema));
        resolver.setRequestContext(context);

        List<Group> groups = readParquetFile("old-repeated-int.parquet", 1, schema);
        List<OneField> fields = assertRow(groups, 0, 1);
        assertEquals(DataType.TEXT.getOID(), fields.get(0).type);
        assertEquals("[1,2,3]", fields.get(0).val);

    }

    private List<OneField> assertRow(List<Group> groups, int desiredRow, int numFields) {
        OneRow row = new OneRow(groups.get(desiredRow)); // get row
        List<OneField> fields = resolver.getFields(row);
        assertEquals(numFields, fields.size());
        return fields;
    }

    private void assertField(List<OneField> fields, int index, Object value, DataType type) {
        assertEquals(type.getOID(), fields.get(index).type);
        if (type == DataType.BYTEA) {
            assertArrayEquals((byte[]) value, (byte[]) fields.get(index).val);
        } else {
            assertEquals(value, fields.get(index).val);
        }
    }

    private MessageType getParquetSchemaForPrimitiveTypes(Type.Repetition repetition, boolean readCase) {
        List<Type> fields = new ArrayList<>();

        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.BINARY, "s1", OriginalType.UTF8));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.BINARY, "s2", OriginalType.UTF8));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.INT32, "n1", null));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.DOUBLE, "d1", null));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 16, "dc1", OriginalType.DECIMAL, new DecimalMetadata(38, 18), null));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.INT96, "tm", null));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.FLOAT, "f", null));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.INT64, "bg", null));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.BOOLEAN, "b", null));

        // GPDB only has int16 and not int8 type, so for write tiny numbers int8 are still treated as shorts in16
        OriginalType tinyType = readCase ? OriginalType.INT_8 : OriginalType.INT_16;
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.INT32, "tn", tinyType));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.INT32, "sml", OriginalType.INT_16));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.BINARY, "vc1", OriginalType.UTF8));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.BINARY, "c1", OriginalType.UTF8));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.BINARY, "bin", null));

        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.INT96, "tmtz", null));
        fields.add(new PrimitiveType(repetition, PrimitiveTypeName.INT96, "tmtz2", null));

        return new MessageType("hive_schema", fields);
    }

    @SuppressWarnings("deprecation")
    private List<Group> readParquetFile(String file, long expectedSize, MessageType schema) throws IOException {
        List<Group> result = new ArrayList<>();
        String parquetFile = Objects.requireNonNull(getClass().getClassLoader().getResource("parquet/" + file)).getPath();
        Path path = new Path(parquetFile);

        ParquetFileReader fileReader = new ParquetFileReader(new Configuration(), path, ParquetMetadataConverter.NO_FILTER);
        PageReadStore rowGroup;
        while ((rowGroup = fileReader.readNextRowGroup()) != null) {
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            RecordReader<Group> recordReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
            long rowCount = rowGroup.getRowCount();
            for (long i = 0; i < rowCount; i++) {
                result.add(recordReader.read());
            }
        }
        fileReader.close();
        assertEquals(expectedSize, result.size());
        return result;
    }

    private List<ColumnDescriptor> getColumnDescriptorsFromSchema(MessageType schema) {
        return schema.getFields()
                .stream()
                .map(f -> {
                    ParquetTypeConverter converter = ParquetTypeConverter.from(f.asPrimitiveType());
                    return new ColumnDescriptor(f.getName(), converter.getDataType(f).getOID(), 1, "", new Integer[]{});
                })
                .collect(Collectors.toList());
    }

    private MessageType buildReadSchema(MessageType originalSchema) {
        List<Type> originalFields = originalSchema.getFields();
        List<Type> projectedFields = new ArrayList<>();
        for (int i = 0; i < context.getTupleDescription().size(); i++) {
            if (context.getTupleDescription().get(i).isProjected()) {
                projectedFields.add(originalFields.get(i));
            }
        }
        return new MessageType(originalSchema.getName(), projectedFields);
    }

    private void testSetFields_RightTrimCharHelper(String varchar, String inputChar, String expectedChar) throws IOException {
        List<Type> typeFields = new ArrayList<>();
        typeFields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "vc1", OriginalType.UTF8));
        typeFields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "c1", OriginalType.UTF8));
        schema = new MessageType("hive_schema", typeFields);
        context.setMetadata(schema);

        List<ColumnDescriptor> columnDescriptors = new ArrayList<>();
        columnDescriptors.add(new ColumnDescriptor("vc1", DataType.VARCHAR.getOID(), 0, "varchar", null));
        columnDescriptors.add(new ColumnDescriptor("c1", DataType.BPCHAR.getOID(), 1, "char", null));
        context.setTupleDescription(columnDescriptors);

        resolver.setRequestContext(context);

        List<OneField> fields = new ArrayList<>();
        fields.add(new OneField(DataType.TEXT.getOID(), varchar));
        // the whitespace on the after 'abc   ' needs to be trimmed
        fields.add(new OneField(DataType.TEXT.getOID(), inputChar));

        OneRow row = resolver.setFields(fields);
        assertNotNull(row);
        Object data = row.getData();
        assertNotNull(data);
        assertTrue(data instanceof Group);
        Group group = (Group) data;

        // assert column values
        assertEquals(varchar, group.getString(0, 0));
        assertEquals(expectedChar, group.getString(1, 0));

        // assert value repetition count
        for (int i = 0; i < 2; i++) {
            assertEquals(1, group.getFieldRepetitionCount(i));
        }
    }
}
