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

import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.ValueSet;

import java.util.List;
import java.util.stream.IntStream;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;

/// Per-file row filtering hint.
/// It is allows skipping irrelevant chunks within a single file - can be shared between several ConnectorPageSource instances.
/// NOT THREAD SAFE!
public class RowFilter
{
    private long filePositionLimit = Long.MAX_VALUE;
    private SortedRangeSet rowsToCollect = (SortedRangeSet) ValueSet.all(BIGINT);

    public static final RowFilter ALL_ROWS = new RowFilter()
    {
        @Override
        public void skipRows(ValueSet rowsToSkip)
        {
        }
    };

    /// Allows skipping multiple small ranges in one call.
    public void skipRows(ValueSet rowsToSkip)
    {
        rowsToCollect = (SortedRangeSet) rowsToCollect.subtract(rowsToSkip);
    }

    public static ValueSet rowRange(long start, long count)
    {
        if (count <= 0) {
            return ValueSet.none(BIGINT);
        }
        Range range = Range.range(BIGINT, start, true, start + count, false);
        return SortedRangeSet.copyOf(BIGINT, List.of(range));
    }

    public boolean isAll()
    {
        return rowsToCollect.isAll();
    }

    public boolean shouldCollectRange(long start, long count)
    {
        return getValidPositions(start, count).findFirst().isPresent();
    }

    /// Returns a stream of block positions that need to be collected in the [start, start + count) range.
    /// A stream is used to allow lazy evaluation of the result.
    /// All types of markers (inclusive/exclusive) are supported for correctness.
    public IntStream getValidPositions(long start, long count)
    {
        List<Range> ranges = rowsToCollect.intersect(rowRange(start, count)).getRanges().getOrderedRanges();
        return IntStream
                .range(0, ranges.size())
                .flatMap(i -> {
                    Range range = ranges.get(i);
                    Marker lowMarker = range.getLow();
                    Marker highMarker = range.getHigh();
                    long first = (Long) lowMarker.getValue() + (lowMarker.getBound() == Marker.Bound.EXACTLY ? 0 : 1);
                    long last = (Long) highMarker.getValue() - (highMarker.getBound() == Marker.Bound.EXACTLY ? 0 : 1);
                    return IntStream.rangeClosed(toIntExact(first - start), toIntExact(last - start));
                });
    }
}
