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
package io.prestosql.parquet.reader;

import org.apache.parquet.hadoop.metadata.BlockMetaData;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ParquetBlockMetaData
        extends BlockMetaData
{
    private final long rowOffset;

    public ParquetBlockMetaData(BlockMetaData metaData, long rowOffset)
    {
        metaData.getColumns().stream().forEach(this::addColumn);
        this.setRowCount(metaData.getRowCount());
        this.setTotalByteSize(metaData.getTotalByteSize());
        this.setPath(metaData.getPath());
        this.rowOffset = rowOffset;
    }

    public long getRowOffset()
    {
        return rowOffset;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("path", getPath())
                .add("totalByteSize", getTotalByteSize())
                .add("rowCount", getRowCount())
                .add("rowOffset", rowOffset)
                .add("columns", getColumns())
                .toString();
    }
}
