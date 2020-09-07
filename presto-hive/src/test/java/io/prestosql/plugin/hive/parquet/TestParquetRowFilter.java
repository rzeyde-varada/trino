/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.hive.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.testing.TempFile;
import io.airlift.units.DataSize;
import io.prestosql.parquet.writer.ParquetSchemaConverter;
import io.prestosql.parquet.writer.ParquetWriter;
import io.prestosql.parquet.writer.ParquetWriterOptions;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.HiveTestUtils;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveTypeName;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.RowFilter;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.prestosql.plugin.hive.HiveType.toHiveType;
import static io.prestosql.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.prestosql.spi.RowFilter.rowRange;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestParquetRowFilter
{
    @Test
    public void testRowFilterPredicate()
            throws Exception
    {
        // build simple Parquet file
        List<Type> types = ImmutableList.of(DOUBLE);
        List<String> columns = ImmutableList.of("col1");
        int rowCount = 100_000;
        int blockSize = 10_000;

        try (TempFile tempFile = new TempFile()) {
            PageBuilder pageBuilder = new PageBuilder(types);
            for (int i = 0; i < types.size(); ++i) {
                Type type = types.get(i);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);

                for (int j = 0; j < rowCount; ++j) {
                    type.writeDouble(blockBuilder, j);
                }
            }
            pageBuilder.declarePositions(rowCount);
            buildParquetFile(tempFile.file(), blockSize, columns, types, ImmutableList.of(pageBuilder.build()));

            List<Double> values = ImmutableList.of(12_345.0, 55_555.0, 98_765.0);
            TupleDomain<String> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                    "col1", Domain.multipleValues(DOUBLE, values)));

            RowFilter rowFilter = new RowFilter();
            ConnectorPageSource pageSource = createPageSource(tempFile.file(), columns, types, predicate, rowFilter);
            assertTrue(pageSource.supportsRowFiltering());

            Set<Integer> blocksToKeep = values.stream().map(value -> value.intValue() / blockSize).collect(Collectors.toSet());
            int[] expected = IntStream.range(0, 10)
                    .filter(blocksToKeep::contains)
                    .flatMap(i -> IntStream.range(i * blockSize, (i + 1) * blockSize))
                    .toArray();
            assertEquals(rowFilter.getValidPositions(0, rowCount).toArray(), expected);
            int rowsCount = 0;
            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page != null) {
                    rowsCount += page.getPositionCount();
                    for (int i = 0; i < page.getChannelCount(); ++i) {
                        Block block = page.getBlock(i);
                        assertEquals(block.getPositionCount(), page.getPositionCount());
                    }
                }
            }
            assertEquals(rowsCount, blockSize * blocksToKeep.size());
        }
    }

    @Test
    public void testRowFilterSelect()
            throws Exception
    {
        // build simple Parquet file
        List<Type> types = ImmutableList.of(DOUBLE);
        List<String> columns = ImmutableList.of("col1");
        int rowCount = 100_000;
        int blockSize = 10_000;

        try (TempFile tempFile = new TempFile()) {
            PageBuilder pageBuilder = new PageBuilder(types);
            for (int i = 0; i < types.size(); ++i) {
                Type type = types.get(i);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);

                for (int j = 0; j < rowCount; ++j) {
                    type.writeDouble(blockBuilder, j);
                }
            }
            pageBuilder.declarePositions(rowCount);
            buildParquetFile(tempFile.file(), blockSize, columns, types, ImmutableList.of(pageBuilder.build()));

            int[] values = new int[] {12_345, 55_555, 98_765};

            RowFilter rowFilter = new RowFilter();
            rowFilter.skipRows(rowRange(0, values[0]));
            rowFilter.skipRows(rowRange(values[0] + 1, values[1] - values[0] - 1));
            rowFilter.skipRows(rowRange(values[1] + 1, values[2] - values[1] - 1));
            rowFilter.skipRows(rowRange(values[2] + 1, rowCount - values[2] - 1));
            ConnectorPageSource pageSource = createPageSource(tempFile.file(), columns, types, TupleDomain.all(), rowFilter);
            assertTrue(pageSource.supportsRowFiltering());

            assertEquals(rowFilter.getValidPositions(0, rowCount).toArray(), values);
            ImmutableList.Builder<Double> results = ImmutableList.builder();
            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page != null) {
                    for (int i = 0; i < page.getChannelCount(); ++i) {
                        Block block = page.getBlock(i);
                        assertEquals(block.getPositionCount(), page.getPositionCount());
                        for (int j = 0; j < block.getPositionCount(); ++j) {
                            results.add(DOUBLE.getDouble(block, j));
                        }
                    }
                }
            }
            assertEquals(results.build(), Arrays.stream(values).mapToObj(v -> (double) v).collect(toImmutableList()));
        }
    }

    private static void buildParquetFile(File targetFie, int blockSize, List<String> columns, List<Type> types, List<Page> pages)
            throws IOException
    {
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(types, columns);
        ParquetWriter writer = new ParquetWriter(
                new FileOutputStream(targetFie),
                schemaConverter.getMessageType(),
                schemaConverter.getPrimitiveTypes(),
                ParquetWriterOptions.builder()
                        .setMaxPageSize(DataSize.ofBytes(100))
                        .setMaxBlockSize(DataSize.ofBytes(blockSize))
                        .build(),
                CompressionCodecName.GZIP);
        for (Page page : pages) {
            writer.write(page);
        }
        writer.close();
    }

    private static ConnectorPageSource createPageSource(File targetFile, List<String> columns, List<Type> types, TupleDomain<String> predicate, RowFilter rowFilter)
    {
        HivePageSourceFactory pageSourceFactory = new ParquetPageSourceFactory(HDFS_ENVIRONMENT, new FileFormatDataSourceStats(), new ParquetReaderConfig(), new HiveConfig());

        Map<String, HiveColumnHandle> columnMap = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i);
            Type columnType = types.get(i);
            columnMap.put(columnName, createBaseColumn(columnName, i, toHiveType(columnType), columnType, REGULAR, Optional.empty()));
        }

        JobConf conf = new JobConf(new Configuration(false));
        conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");

        return pageSourceFactory
                .createPageSource(
                        conf,
                        HiveTestUtils.SESSION,
                        new Path(targetFile.getAbsolutePath()),
                        0,
                        targetFile.length(),
                        targetFile.length(),
                        createSchema(HiveStorageFormat.PARQUET, columns, types),
                        columns.stream().map(columnMap::get).collect(toImmutableList()),
                        predicate.transform(columnMap::get),
                        Optional.empty(),
                        OptionalInt.empty(),
                        true,
                        NO_ACID_TRANSACTION,
                        rowFilter)
                .get().get();
    }

    private static Properties createSchema(HiveStorageFormat format, List<String> columnNames, List<Type> columnTypes)
    {
        Properties schema = new Properties();
        schema.setProperty(SERIALIZATION_LIB, format.getSerDe());
        schema.setProperty(FILE_INPUT_FORMAT, format.getInputFormat());
        schema.setProperty(META_TABLE_COLUMNS, columnNames.stream()
                .collect(joining(",")));
        schema.setProperty(META_TABLE_COLUMN_TYPES, columnTypes.stream()
                .map(type -> toHiveType(type))
                .map(HiveType::getHiveTypeName)
                .map(HiveTypeName::toString)
                .collect(joining(":")));
        return schema;
    }
}
