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
package io.prestosql.spi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.ValueSet;
import org.testng.annotations.Test;

import java.util.stream.IntStream;

import static io.prestosql.spi.RowFilter.ALL_ROWS;
import static io.prestosql.spi.RowFilter.rowRange;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

public class TestRowFilter
{
    @Test
    public void testAll()
    {
        RowFilter rowFilter = RowFilter.ALL_ROWS;
        assertTrue(rowFilter.isAll());
        assertTrue(rowFilter.shouldCollectRange(0, 10));
        assertArrayEquals(rowFilter.getValidPositions(0, 10).toArray(), IntStream.range(0, 10).toArray());

        rowFilter.skipRows(rowRange(4, 4));  // should be ignored
        assertTrue(rowFilter.isAll());
        assertTrue(rowFilter.shouldCollectRange(0, 10));
        assertArrayEquals(rowFilter.getValidPositions(0, 10).toArray(), IntStream.range(0, 10).toArray());
    }

    @Test
    public void testFilter()
    {
        RowFilter rowFilter = new RowFilter();
        assertTrue(rowFilter.isAll());
        assertTrue(rowFilter.shouldCollectRange(0, 10));
        assertFalse(rowFilter.shouldCollectRange(5, 0));
        assertArrayEquals(rowFilter.getValidPositions(0, 10).toArray(), IntStream.range(0, 10).toArray());

        rowFilter.skipRows(rowRange(4, 2));
        assertFalse(rowFilter.isAll());
        assertTrue(rowFilter.shouldCollectRange(0, 10));
        assertFalse(rowFilter.shouldCollectRange(4, 2));
        assertFalse(rowFilter.shouldCollectRange(4, 1));
        assertFalse(rowFilter.shouldCollectRange(5, 1));
        assertArrayEquals(
                rowFilter.getValidPositions(0, 10).toArray(),
                Streams.concat(IntStream.range(0, 4), IntStream.range(6, 10)).toArray());

        rowFilter.skipRows(rowRange(0, 1));
        assertFalse(rowFilter.isAll());
        assertTrue(rowFilter.shouldCollectRange(0, 10));
        assertFalse(rowFilter.shouldCollectRange(4, 2));
        assertFalse(rowFilter.shouldCollectRange(0, 1));
        assertArrayEquals(
                rowFilter.getValidPositions(0, 10).toArray(),
                Streams.concat(IntStream.range(1, 4), IntStream.range(6, 10)).toArray());

        rowFilter.skipRows(rowRange(5, 5));
        assertFalse(rowFilter.isAll());
        assertTrue(rowFilter.shouldCollectRange(0, 10));
        assertFalse(rowFilter.shouldCollectRange(4, 6));
        assertFalse(rowFilter.shouldCollectRange(0, 1));
        assertArrayEquals(
                rowFilter.getValidPositions(0, 10).toArray(),
                IntStream.range(1, 4).toArray());

        rowFilter.skipRows(rowRange(1, 3));
        assertFalse(rowFilter.isAll());
        assertFalse(rowFilter.shouldCollectRange(0, 10));
        assertArrayEquals(rowFilter.getValidPositions(0, 10).toArray(), new int[0]);

        rowFilter.skipRows(ValueSet.copyOf(BIGINT, ImmutableList.of(11L, 15L, 16L, 17L, 19L)));
        assertFalse(rowFilter.isAll());
        assertTrue(rowFilter.shouldCollectRange(10, 10));
        assertArrayEquals(rowFilter.getValidPositions(10, 10).toArray(), new int[] {0, 2, 3, 4, 8});

        rowFilter.skipRows(ValueSet.copyOfRanges(BIGINT, ImmutableList.of(Range.lessThan(BIGINT, 13L))));
        assertFalse(rowFilter.isAll());
        assertTrue(rowFilter.shouldCollectRange(10, 10));
        assertArrayEquals(rowFilter.getValidPositions(10, 10).toArray(), new int[] {3, 4, 8});

        rowFilter.skipRows(ValueSet.copyOfRanges(BIGINT, ImmutableList.of(Range.greaterThanOrEqual(BIGINT, 14L))));
        assertFalse(rowFilter.isAll());
        assertTrue(rowFilter.shouldCollectRange(10, 10));
        assertArrayEquals(rowFilter.getValidPositions(10, 10).toArray(), new int[] {3});
    }
}
