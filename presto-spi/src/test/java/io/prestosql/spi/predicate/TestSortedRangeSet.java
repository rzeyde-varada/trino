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
package io.prestosql.spi.predicate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.TestingBlockEncodingSerde;
import io.prestosql.spi.block.TestingBlockJsonSerde;
import io.prestosql.spi.type.TestingTypeDeserializer;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.Type;
import org.assertj.core.api.AssertProvider;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSortedRangeSet
{
    @Test
    public void testEmptySet()
    {
        SortedRangeSet rangeSet = SortedRangeSet.none(BIGINT);
        assertEquals(rangeSet.getType(), BIGINT);
        assertTrue(rangeSet.isNone());
        assertFalse(rangeSet.isAll());
        assertFalse(rangeSet.isSingleValue());
        assertTrue(Iterables.isEmpty(rangeSet.getOrderedRanges()));
        assertEquals(rangeSet.getRangeCount(), 0);
        assertEquals(rangeSet.complement(), SortedRangeSet.all(BIGINT));
        assertFalse(rangeSet.includesMarker(Marker.lowerUnbounded(BIGINT)));
        assertFalse(rangeSet.includesMarker(Marker.exactly(BIGINT, 0L)));
        assertFalse(rangeSet.includesMarker(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testEntireSet()
    {
        SortedRangeSet rangeSet = SortedRangeSet.all(BIGINT);
        assertEquals(rangeSet.getType(), BIGINT);
        assertFalse(rangeSet.isNone());
        assertTrue(rangeSet.isAll());
        assertFalse(rangeSet.isSingleValue());
        assertEquals(rangeSet.getRangeCount(), 1);
        assertEquals(rangeSet.complement(), SortedRangeSet.none(BIGINT));
        assertTrue(rangeSet.includesMarker(Marker.lowerUnbounded(BIGINT)));
        assertTrue(rangeSet.includesMarker(Marker.exactly(BIGINT, 0L)));
        assertTrue(rangeSet.includesMarker(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testSingleValue()
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(BIGINT, 10L);

        SortedRangeSet complement = SortedRangeSet.of(Range.greaterThan(BIGINT, 10L), Range.lessThan(BIGINT, 10L));

        assertEquals(rangeSet.getType(), BIGINT);
        assertFalse(rangeSet.isNone());
        assertFalse(rangeSet.isAll());
        assertTrue(rangeSet.isSingleValue());
        assertTrue(Iterables.elementsEqual(rangeSet.getOrderedRanges(), ImmutableList.of(Range.equal(BIGINT, 10L))));
        assertEquals(rangeSet.getRangeCount(), 1);
        assertEquals(rangeSet.complement(), complement);
        assertFalse(rangeSet.includesMarker(Marker.lowerUnbounded(BIGINT)));
        assertTrue(rangeSet.includesMarker(Marker.exactly(BIGINT, 10L)));
        assertFalse(rangeSet.includesMarker(Marker.exactly(BIGINT, 9L)));
        assertFalse(rangeSet.includesMarker(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testBoundedSet()
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(
                Range.equal(BIGINT, 10L),
                Range.equal(BIGINT, 0L),
                Range.range(BIGINT, 9L, true, 11L, false),
                Range.equal(BIGINT, 0L),
                Range.range(BIGINT, 2L, true, 4L, true),
                Range.range(BIGINT, 4L, false, 5L, true));

        ImmutableList<Range> normalizedResult = ImmutableList.of(
                Range.equal(BIGINT, 0L),
                Range.range(BIGINT, 2L, true, 5L, true),
                Range.range(BIGINT, 9L, true, 11L, false));

        SortedRangeSet complement = SortedRangeSet.of(
                Range.lessThan(BIGINT, 0L),
                Range.range(BIGINT, 0L, false, 2L, false),
                Range.range(BIGINT, 5L, false, 9L, false),
                Range.greaterThanOrEqual(BIGINT, 11L));

        assertEquals(rangeSet.getType(), BIGINT);
        assertFalse(rangeSet.isNone());
        assertFalse(rangeSet.isAll());
        assertFalse(rangeSet.isSingleValue());
        assertTrue(Iterables.elementsEqual(rangeSet.getOrderedRanges(), normalizedResult));
        assertEquals(rangeSet, SortedRangeSet.copyOf(BIGINT, normalizedResult));
        assertEquals(rangeSet.getRangeCount(), 3);
        assertEquals(rangeSet.complement(), complement);
        assertFalse(rangeSet.includesMarker(Marker.lowerUnbounded(BIGINT)));
        assertTrue(rangeSet.includesMarker(Marker.exactly(BIGINT, 0L)));
        assertFalse(rangeSet.includesMarker(Marker.exactly(BIGINT, 1L)));
        assertFalse(rangeSet.includesMarker(Marker.exactly(BIGINT, 7L)));
        assertTrue(rangeSet.includesMarker(Marker.exactly(BIGINT, 9L)));
        assertFalse(rangeSet.includesMarker(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testUnboundedSet()
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(
                Range.greaterThan(BIGINT, 10L),
                Range.lessThanOrEqual(BIGINT, 0L),
                Range.range(BIGINT, 2L, true, 4L, false),
                Range.range(BIGINT, 4L, true, 6L, false),
                Range.range(BIGINT, 1L, false, 2L, false),
                Range.range(BIGINT, 9L, false, 11L, false));

        ImmutableList<Range> normalizedResult = ImmutableList.of(
                Range.lessThanOrEqual(BIGINT, 0L),
                Range.range(BIGINT, 1L, false, 6L, false),
                Range.greaterThan(BIGINT, 9L));

        SortedRangeSet complement = SortedRangeSet.of(
                Range.range(BIGINT, 0L, false, 1L, true),
                Range.range(BIGINT, 6L, true, 9L, true));

        assertEquals(rangeSet.getType(), BIGINT);
        assertFalse(rangeSet.isNone());
        assertFalse(rangeSet.isAll());
        assertFalse(rangeSet.isSingleValue());
        assertTrue(Iterables.elementsEqual(rangeSet.getOrderedRanges(), normalizedResult));
        assertEquals(rangeSet, SortedRangeSet.copyOf(BIGINT, normalizedResult));
        assertEquals(rangeSet.getRangeCount(), 3);
        assertEquals(rangeSet.complement(), complement);
        assertTrue(rangeSet.includesMarker(Marker.lowerUnbounded(BIGINT)));
        assertTrue(rangeSet.includesMarker(Marker.exactly(BIGINT, 0L)));
        assertTrue(rangeSet.includesMarker(Marker.exactly(BIGINT, 4L)));
        assertFalse(rangeSet.includesMarker(Marker.exactly(BIGINT, 7L)));
        assertTrue(rangeSet.includesMarker(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testGetSingleValue()
    {
        assertEquals(SortedRangeSet.of(BIGINT, 0L).getSingleValue(), 0L);
        assertThatThrownBy(() -> SortedRangeSet.all(BIGINT).getSingleValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("SortedRangeSet does not have just a single value");
    }

    @Test
    public void testSpan()
    {
        assertThatThrownBy(() -> SortedRangeSet.none(BIGINT).getSpan())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot get span if no ranges exist");

        assertEquals(SortedRangeSet.all(BIGINT).getSpan(), Range.all(BIGINT));
        assertEquals(SortedRangeSet.of(BIGINT, 0L).getSpan(), Range.equal(BIGINT, 0L));
        assertEquals(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).getSpan(), Range.range(BIGINT, 0L, true, 1L, true));
        assertEquals(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.greaterThan(BIGINT, 1L)).getSpan(), Range.greaterThanOrEqual(BIGINT, 0L));
        assertEquals(SortedRangeSet.of(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 1L)).getSpan(), Range.all(BIGINT));
    }

    @Test
    public void testOverlaps()
    {
        assertTrue(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.all(BIGINT)));
        assertFalse(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.none(BIGINT)));
        assertTrue(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.of(BIGINT, 0L)));
        assertTrue(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))));
        assertTrue(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
        assertTrue(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L))));

        assertFalse(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.all(BIGINT)));
        assertFalse(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.none(BIGINT)));
        assertFalse(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.of(BIGINT, 0L)));
        assertFalse(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))));
        assertFalse(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
        assertFalse(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L))));

        assertTrue(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.all(BIGINT)));
        assertFalse(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.none(BIGINT)));
        assertTrue(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.of(BIGINT, 0L)));
        assertTrue(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))));
        assertFalse(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
        assertFalse(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L))));

        assertTrue(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).overlaps(SortedRangeSet.of(Range.equal(BIGINT, 1L))));
        assertFalse(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).overlaps(SortedRangeSet.of(Range.equal(BIGINT, 2L))));
        assertTrue(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
        assertTrue(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).overlaps(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L))));
        assertFalse(SortedRangeSet.of(Range.lessThan(BIGINT, 0L)).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
    }

    @Test
    public void testContains()
    {
        assertTrue(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.all(BIGINT)));
        assertTrue(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.none(BIGINT)));
        assertTrue(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.of(BIGINT, 0L)));
        assertTrue(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))));
        assertTrue(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
        assertTrue(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L))));

        assertFalse(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.all(BIGINT)));
        assertTrue(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.none(BIGINT)));
        assertFalse(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.of(BIGINT, 0L)));
        assertFalse(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))));
        assertFalse(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
        assertFalse(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L))));

        assertFalse(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.all(BIGINT)));
        assertTrue(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.none(BIGINT)));
        assertTrue(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.of(BIGINT, 0L)));
        assertFalse(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))));
        assertFalse(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
        assertFalse(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L))));

        assertTrue(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).contains(SortedRangeSet.of(Range.equal(BIGINT, 1L))));
        assertFalse(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).contains(SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L))));
        assertTrue(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
        assertFalse(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).contains(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L))));
        assertFalse(SortedRangeSet.of(Range.lessThan(BIGINT, 0L)).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
    }

    @Test
    public void testContainsValue()
    {
        // BIGINT all
        assertSortedRangeSet(SortedRangeSet.all(BIGINT))
                .containsValue(Long.MIN_VALUE)
                .containsValue(0L)
                .containsValue(42L)
                .containsValue(Long.MAX_VALUE);

        // BIGINT range
        assertSortedRangeSet(SortedRangeSet.of(Range.range(BIGINT, 10L, true, 41L, true)))
                .doesNotContainValue(9L)
                .containsValue(10L)
                .containsValue(11L)
                .containsValue(30L)
                .containsValue(41L)
                .doesNotContainValue(42L);

        assertSortedRangeSet(SortedRangeSet.of(Range.range(BIGINT, 10L, false, 41L, false)))
                .doesNotContainValue(10L)
                .containsValue(11L)
                .containsValue(40L)
                .doesNotContainValue(41L);

        // REAL all
        assertSortedRangeSet(SortedRangeSet.all(REAL))
                .containsValue((long) floatToRawIntBits(42.0f))
                .containsValue((long) floatToRawIntBits(Float.NaN));

        // REAL range
        assertSortedRangeSet(SortedRangeSet.of(Range.range(REAL, (long) floatToRawIntBits(10.0f), true, (long) floatToRawIntBits(41.0f), true)))
                .doesNotContainValue((long) floatToRawIntBits(9.999999f))
                .containsValue((long) floatToRawIntBits(10.0f))
                .containsValue((long) floatToRawIntBits(41.0f))
                .doesNotContainValue((long) floatToRawIntBits(41.00001f))
                .doesNotContainValue((long) floatToRawIntBits(Float.NaN));

        assertSortedRangeSet(SortedRangeSet.of(Range.range(REAL, (long) floatToRawIntBits(10.0f), false, (long) floatToRawIntBits(41.0f), false)))
                .doesNotContainValue((long) floatToRawIntBits(10.0f))
                .containsValue((long) floatToRawIntBits(10.00001f))
                .containsValue((long) floatToRawIntBits(40.99999f))
                .doesNotContainValue((long) floatToRawIntBits(41.0f))
                .doesNotContainValue((long) floatToRawIntBits(Float.NaN));

        // DOUBLE all
        assertSortedRangeSet(SortedRangeSet.all(DOUBLE))
                .containsValue(42.0)
                .containsValue(Double.NaN);

        // DOUBLE range
        assertSortedRangeSet(SortedRangeSet.of(Range.range(DOUBLE, 10.0, true, 41.0, true)))
                .doesNotContainValue(9.999999999999999)
                .containsValue(10.0)
                .containsValue(41.0)
                .doesNotContainValue(41.00000000000001)
                .doesNotContainValue(Double.NaN);

        assertSortedRangeSet(SortedRangeSet.of(Range.range(DOUBLE, 10.0, false, 41.0, false)))
                .doesNotContainValue(10.0)
                .containsValue(10.00000000000001)
                .containsValue(40.99999999999999)
                .doesNotContainValue(41.0)
                .doesNotContainValue(Double.NaN);
    }

    @Test
    public void testIntersect()
    {
        assertEquals(
                SortedRangeSet.none(BIGINT).intersect(
                        SortedRangeSet.none(BIGINT)),
                SortedRangeSet.none(BIGINT));

        assertEquals(
                SortedRangeSet.all(BIGINT).intersect(
                        SortedRangeSet.all(BIGINT)),
                SortedRangeSet.all(BIGINT));

        assertEquals(
                SortedRangeSet.none(BIGINT).intersect(
                        SortedRangeSet.all(BIGINT)),
                SortedRangeSet.none(BIGINT));

        assertEquals(
                SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)).intersect(
                        SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.equal(BIGINT, 4L))),
                SortedRangeSet.of(Range.equal(BIGINT, 2L)));

        assertEquals(
                SortedRangeSet.all(BIGINT).intersect(
                        SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.equal(BIGINT, 4L))),
                SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.equal(BIGINT, 4L)));

        assertEquals(
                SortedRangeSet.of(Range.range(BIGINT, 0L, true, 4L, false)).intersect(
                        SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.greaterThan(BIGINT, 3L))),
                SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.range(BIGINT, 3L, false, 4L, false)));

        assertEquals(
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)).intersect(
                        SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 0L))),
                SortedRangeSet.of(Range.equal(BIGINT, 0L)));

        assertEquals(
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, -1L)).intersect(
                        SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 1L))),
                SortedRangeSet.of(Range.range(BIGINT, -1L, true, 1L, true)));
    }

    @Test
    public void testUnion()
    {
        assertUnion(SortedRangeSet.none(BIGINT), SortedRangeSet.none(BIGINT), SortedRangeSet.none(BIGINT));
        assertUnion(SortedRangeSet.all(BIGINT), SortedRangeSet.all(BIGINT), SortedRangeSet.all(BIGINT));
        assertUnion(SortedRangeSet.none(BIGINT), SortedRangeSet.all(BIGINT), SortedRangeSet.all(BIGINT));

        assertUnion(
                SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)),
                SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)),
                SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)));

        assertUnion(SortedRangeSet.all(BIGINT), SortedRangeSet.of(Range.equal(BIGINT, 0L)), SortedRangeSet.all(BIGINT));

        assertUnion(
                SortedRangeSet.of(Range.range(BIGINT, 0L, true, 4L, false)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 3L)),
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)));

        assertUnion(
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)),
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 0L)),
                SortedRangeSet.of(Range.all(BIGINT)));

        assertUnion(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)),
                SortedRangeSet.of(Range.lessThan(BIGINT, 0L)),
                SortedRangeSet.of(BIGINT, 0L).complement());
    }

    @Test
    public void testSubtract()
    {
        assertEquals(
                SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.all(BIGINT)),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.none(BIGINT)),
                SortedRangeSet.all(BIGINT));
        assertEquals(
                SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.of(BIGINT, 0L)),
                SortedRangeSet.of(BIGINT, 0L).complement());
        assertEquals(
                SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))),
                SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).complement());
        assertEquals(
                SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))),
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 0L)));

        assertEquals(
                SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.all(BIGINT)),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.none(BIGINT)),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.of(BIGINT, 0L)),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))),
                SortedRangeSet.none(BIGINT));

        assertEquals(
                SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.all(BIGINT)),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.none(BIGINT)),
                SortedRangeSet.of(BIGINT, 0L));
        assertEquals(
                SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.of(BIGINT, 0L)),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))),
                SortedRangeSet.of(BIGINT, 0L));

        assertEquals(
                SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.all(BIGINT)),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.none(BIGINT)),
                SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)));
        assertEquals(
                SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.of(BIGINT, 0L)),
                SortedRangeSet.of(BIGINT, 1L));
        assertEquals(
                SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))),
                SortedRangeSet.of(Range.equal(BIGINT, 0L)));

        assertEquals(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.all(BIGINT)),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.none(BIGINT)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)));
        assertEquals(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.of(BIGINT, 0L)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)));
        assertEquals(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))),
                SortedRangeSet.of(Range.range(BIGINT, 0L, false, 1L, false), Range.greaterThan(BIGINT, 1L)));
        assertEquals(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))),
                SortedRangeSet.none(BIGINT));
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

        ObjectMapper mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)));

        SortedRangeSet set = SortedRangeSet.all(BIGINT);
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));

        set = SortedRangeSet.none(DOUBLE);
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));

        set = SortedRangeSet.of(VARCHAR, utf8Slice("abc"));
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));

        set = SortedRangeSet.of(Range.equal(BOOLEAN, true), Range.equal(BOOLEAN, false));
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));
    }

    @Test
    public void testSuffix()
    {
        SortedRangeSet suffixes = likePatterns("%123", "%45", "%6");
        assertEquals(suffixes.getStringMatchers().getLikePatterns(), Sets.newHashSet("%123", "%45", "%6"));
        assertTrue(suffixes.getOrderedRanges().isEmpty());
        assertEquals(suffixes.isNone(), false);
        assertEquals(suffixes.isAll(), false);

        SortedRangeSet empty = likePatterns();
        assertTrue(empty.getStringMatchers().getLikePatterns().isEmpty());
        assertEquals(empty.isNone(), true);
        assertEquals(suffixes.isAll(), false);

        assertEquals(likePatterns("%123").union(likePatterns("%456")).getStringMatchers().getLikePatterns(),
                     Sets.newHashSet("%123", "%456"));
        assertEquals(likePatterns("%123", "%234").union(likePatterns("%234", "%345")).getStringMatchers().getLikePatterns(),
                     Sets.newHashSet("%123", "%234", "%345"));

        assertEquals(likePatterns("%123").union(SortedRangeSet.none(VARCHAR)), likePatterns("%123"));
        assertEquals(likePatterns("%123").union(SortedRangeSet.all(VARCHAR)), SortedRangeSet.all(VARCHAR));
        assertEquals(SortedRangeSet.none(VARCHAR).union(likePatterns("%123")), likePatterns("%123"));
        assertEquals(SortedRangeSet.all(VARCHAR).union(likePatterns("%123")), SortedRangeSet.all(VARCHAR));

        assertEquals(likePatterns("%123").intersect(SortedRangeSet.none(VARCHAR)), SortedRangeSet.none(VARCHAR));
        assertEquals(SortedRangeSet.none(VARCHAR).intersect(likePatterns("%123")), SortedRangeSet.none(VARCHAR));
        // Note: the following results are suboptimal.
        assertEquals(likePatterns("%123").intersect(SortedRangeSet.all(VARCHAR)), SortedRangeSet.all(VARCHAR));
        assertEquals(SortedRangeSet.all(VARCHAR).intersect(likePatterns("%123")), SortedRangeSet.all(VARCHAR));

        ValueSet r1 = ValueSet.of(VARCHAR, utf8Slice("a"), utf8Slice("b"));
        ValueSet r2 = ValueSet.of(VARCHAR, utf8Slice("b"), utf8Slice("c"));
        ValueSet s1 = likePatterns("%x", "%y");
        ValueSet s2 = likePatterns("%y", "%z");

        assertEquals(r1.union(r2), ValueSet.of(VARCHAR, utf8Slice("a"), utf8Slice("b"), utf8Slice("c")));
        assertEquals(r2.union(r1), ValueSet.of(VARCHAR, utf8Slice("a"), utf8Slice("b"), utf8Slice("c")));
        assertEquals(s1.union(s2), likePatterns("%x", "%y", "%z"));
        assertEquals(s2.union(s1), likePatterns("%x", "%y", "%z"));
        assertEquals(r1.intersect(r2), ValueSet.of(VARCHAR, utf8Slice("b")));
        assertEquals(r2.intersect(r1), ValueSet.of(VARCHAR, utf8Slice("b")));
        assertEquals(s1.intersect(s2), s2);
        assertEquals(s2.intersect(s1), s1);

        assertEquals(r1.intersect(s1), r1);
        assertEquals(r1.intersect(s2), r1);
        assertEquals(r2.intersect(s1), r2);
        assertEquals(r2.intersect(s2), r2);

        assertEquals(s1.intersect(r1), r1);
        assertEquals(s1.intersect(r2), r2);
        assertEquals(s2.intersect(r1), r1);
        assertEquals(s2.intersect(r2), r2);

        assertEquals(r2.union(s2).intersect(r1), r1);
        assertEquals(r1.union(s1).intersect(r2), r2);
        assertEquals(r1.intersect(r2.union(s2)), r1);
        assertEquals(r2.intersect(r1.union(s1)), r2);

        assertEquals(r1.union(s1).intersect(r2.union(s2)), r1.union(r2).union(s2));
        assertEquals(r2.union(s2).intersect(r1.union(s1)), r1.union(r2).union(s1));
    }

    @Test
    public void testPrefix()
    {
        SortedRangeSet prefixes = likePatterns("123%", "45%", "6%");
        assertEquals(prefixes.getStringMatchers().getLikePatterns(), Sets.newHashSet("123%", "45%", "6%"));
        assertTrue(prefixes.getOrderedRanges().isEmpty());
        assertFalse(prefixes.isNone());
        assertFalse(prefixes.isAll());

        SortedRangeSet empty = likePatterns();
        assertTrue(empty.getStringMatchers().getLikePatterns().isEmpty());
        assertTrue(empty.isNone());
        assertFalse(prefixes.isAll());

        assertEquals(likePatterns("123%").union(likePatterns("456%")).getStringMatchers().getLikePatterns(),
                     Sets.newHashSet("123%", "456%"));
        assertEquals(likePatterns("123%", "234%").union(likePatterns("234%", "345%")).getStringMatchers().getLikePatterns(),
                     Sets.newHashSet("123%", "234%", "345%"));

        assertEquals(likePatterns("123%").union(SortedRangeSet.none(VARCHAR)), likePatterns("123%"));
        assertEquals(likePatterns("123%").union(SortedRangeSet.all(VARCHAR)), SortedRangeSet.all(VARCHAR));
        assertEquals(SortedRangeSet.none(VARCHAR).union(likePatterns("123%")), likePatterns("123%"));
        assertEquals(SortedRangeSet.all(VARCHAR).union(likePatterns("123%")), SortedRangeSet.all(VARCHAR));

        assertEquals(likePatterns("123%").intersect(SortedRangeSet.none(VARCHAR)), SortedRangeSet.none(VARCHAR));
        assertEquals(SortedRangeSet.none(VARCHAR).intersect(likePatterns("123%")), SortedRangeSet.none(VARCHAR));
        // Note: the following results are suboptimal.
        assertEquals(likePatterns("123%").intersect(SortedRangeSet.all(VARCHAR)), SortedRangeSet.all(VARCHAR));
        assertEquals(SortedRangeSet.all(VARCHAR).intersect(likePatterns("123%")), SortedRangeSet.all(VARCHAR));

        ValueSet r1 = ValueSet.of(VARCHAR, utf8Slice("a"), utf8Slice("b"));
        ValueSet r2 = ValueSet.of(VARCHAR, utf8Slice("b"), utf8Slice("c"));
        ValueSet s1 = likePatterns("x%", "y%");
        ValueSet s2 = likePatterns("y%", "z%");

        assertEquals(r1.union(r2), ValueSet.of(VARCHAR, utf8Slice("a"), utf8Slice("b"), utf8Slice("c")));
        assertEquals(r2.union(r1), ValueSet.of(VARCHAR, utf8Slice("a"), utf8Slice("b"), utf8Slice("c")));
        assertEquals(s1.union(s2), likePatterns("x%", "y%", "z%"));
        assertEquals(s2.union(s1), likePatterns("x%", "y%", "z%"));
        assertEquals(r1.intersect(r2), ValueSet.of(VARCHAR, utf8Slice("b")));
        assertEquals(r2.intersect(r1), ValueSet.of(VARCHAR, utf8Slice("b")));
        assertEquals(s1.intersect(s2), s2);
        assertEquals(s2.intersect(s1), s1);

        assertEquals(r1.intersect(s1), r1);
        assertEquals(r1.intersect(s2), r1);
        assertEquals(r2.intersect(s1), r2);
        assertEquals(r2.intersect(s2), r2);

        assertEquals(s1.intersect(r1), r1);
        assertEquals(s1.intersect(r2), r2);
        assertEquals(s2.intersect(r1), r1);
        assertEquals(s2.intersect(r2), r2);

        assertEquals(r2.union(s2).intersect(r1), r1);
        assertEquals(r1.union(s1).intersect(r2), r2);
        assertEquals(r1.intersect(r2.union(s2)), r1);
        assertEquals(r2.intersect(r1.union(s1)), r2);

        assertEquals(r1.union(s1).intersect(r2.union(s2)), r1.union(r2).union(s2));
        assertEquals(r2.union(s2).intersect(r1.union(s1)), r1.union(r2).union(s1));
    }

    @Test
    public void testPrefixAndSuffix()
    {
        SortedRangeSet sortedRangeSet = likePatterns("%123", "%456");
        sortedRangeSet = likePatterns(sortedRangeSet, "abc%", "def%");
        assertEquals(sortedRangeSet.getStringMatchers().getLikePatterns(), Sets.newHashSet("abc%", "def%", "%123", "%456"));
        assertTrue(sortedRangeSet.getOrderedRanges().isEmpty());
        assertFalse(sortedRangeSet.isNone());
        assertFalse(sortedRangeSet.isAll());

        SortedRangeSet suffixSet = likePatterns("%123", "%456");
        SortedRangeSet prefixSet = likePatterns("abc%", "def%");
        SortedRangeSet union = suffixSet.union(prefixSet);
        assertEquals(sortedRangeSet.getStringMatchers().getLikePatterns(), Sets.newHashSet("abc%", "def%", "%123", "%456"));

        assertEquals(union.intersect(SortedRangeSet.all(VARCHAR)), SortedRangeSet.all(VARCHAR));
        assertEquals((SortedRangeSet.all(VARCHAR).intersect(union)), SortedRangeSet.all(VARCHAR));

        SortedRangeSet r1 = SortedRangeSet.of(VARCHAR, utf8Slice("a"), utf8Slice("b"));

        SortedRangeSet expected = new SortedRangeSet.Builder(VARCHAR).addAll(r1.getOrderedRanges()).build();
        SortedRangeSet intersect = union.intersect(r1);
        assertEquals(expected, intersect);

        r1 = SortedRangeSet.of(VARCHAR, utf8Slice("R1"));
        SortedRangeSet r2 = SortedRangeSet.of(VARCHAR, utf8Slice("R2"));
        SortedRangeSet s1 = likePatterns("%S1");
        SortedRangeSet s2 = likePatterns("%S2");
        SortedRangeSet p1 = likePatterns("P1%");
        SortedRangeSet p2 = likePatterns("P2%");

        SortedRangeSet actual = s1.union(r1).intersect(r2);
        assertEquals(actual, r2);
        actual = s1.union(p1).intersect(r2);
        assertEquals(actual, r2);
        actual = s1.union(r1).union(p1).intersect(r2);
        assertEquals(actual, r2);
        actual = p1.union(r1).intersect(r2);
        assertEquals(actual, r2);

        actual = s1.union(r1).intersect(s2);
        assertEquals(actual, r1.union(s2));
        actual = s1.union(p1).intersect(s2);
        assertEquals(actual, s2);
        actual = s1.union(r1).union(p1).intersect(s2);
        assertEquals(actual, r1.union(s2));
        actual = p1.union(r1).intersect(s2);
        assertEquals(actual, r1.union(s2));

        actual = s1.union(r1).intersect(p2);
        assertEquals(actual, r1.union(p2));
        actual = s1.union(p1).intersect(p2);
        assertEquals(actual, p2);
        actual = s1.union(r1).union(p1).intersect(p2);
        assertEquals(actual, r1.union(p2));
        actual = p1.union(r1).intersect(p2);
        assertEquals(actual, p2.union(r1));

        actual = s1.union(r1).intersect(s2.union(r2));
        assertEquals(actual, r1.union(r2).union(s2));
        actual = s1.union(p1).intersect(s2.union(r2));
        assertEquals(actual, s2.union(r2));
        actual = s1.union(r1).union(p1).intersect(s2.union(r2));
        assertEquals(actual, s2.union(r1).union(r2));
        actual = p1.union(r1).intersect(s2.union(r2));
        assertEquals(actual, r2.union(r1).union(s2));

        actual = s1.union(r1).intersect(s2.union(p2));
        assertEquals(actual, r1.union(s1));
        actual = s1.union(p1).intersect(s2.union(p2));
        assertEquals(actual, s2.union(p2));
        actual = s1.union(r1).union(p1).intersect(s2.union(p2));
        assertEquals(actual, s2.union(r1).union(p2));
        actual = p1.union(r1).intersect(s2.union(p2));
        assertEquals(actual, p1.union(r1));

        actual = s1.union(r1).intersect(s2.union(r2).union(p2));
        assertEquals(actual, r1.union(r2).union(s1));
        actual = s1.union(p1).intersect(s2.union(r2).union(p2));
        assertEquals(actual, r2.union(p2).union(s2));
        actual = s1.union(r1).union(p1).intersect(s2.union(r2).union(p2));
        assertEquals(actual, r1.union(r2).union(s2).union(p2));
        actual = p1.union(r1).intersect(s2.union(r2).union(p2));
        assertEquals(actual, r1.union(r2).union(p1));

        // Choose shorter prefix/suffix during intersection (as a heuristic):
        actual = p1.union(p2).intersect(s1);
        assertEquals(actual, s1);
        actual = s1.intersect(p1.union(p2));
        assertEquals(actual, s1);

        actual = p1.union(p2).intersect(p1);
        assertEquals(actual, p1);
        actual = p1.intersect(p1.union(p2));
        assertEquals(actual, p1);

        actual = s1.union(s2).intersect(p1);
        assertEquals(actual, p1);
        actual = p1.intersect(s1.union(s2));
        assertEquals(actual, p1);

        actual = s1.union(s2).intersect(s1);
        assertEquals(actual, s1);
        actual = s1.intersect(s1.union(s2));
        assertEquals(actual, s1);
    }

    @Test
    public void testRegexpLike()
    {
        SortedRangeSet prefixes = regexLikePatterns("^12\\s3.*a$", "b+b.*", ".*a.*");
        assertEquals(prefixes.getStringMatchers().getRegexpLikePatterns(), Sets.newHashSet("^12\\s3.*a$", "b+b.*", ".*a.*"));
        assertTrue(prefixes.getOrderedRanges().isEmpty());
        assertFalse(prefixes.isNone());
        assertFalse(prefixes.isAll());

        SortedRangeSet empty = regexLikePatterns();
        assertTrue(empty.getStringMatchers().getRegexpLikePatterns().isEmpty());
        assertTrue(empty.isNone());
        assertFalse(prefixes.isAll());

        assertEquals(regexLikePatterns(".*123.*").union(regexLikePatterns(".*456.*")).getStringMatchers().getRegexpLikePatterns(),
                     Sets.newHashSet(".*123.*", ".*456.*"));
        assertEquals(regexLikePatterns(".*123.*", ".*234.*").union(regexLikePatterns(".*234.*", ".*345.*")).getStringMatchers().getRegexpLikePatterns(),
                     Sets.newHashSet(".*123.*", ".*234.*", ".*345.*"));

        assertEquals(regexLikePatterns(".*123.*").union(SortedRangeSet.none(VARCHAR)), regexLikePatterns(".*123.*"));
        assertEquals(regexLikePatterns(".*123.*").union(SortedRangeSet.all(VARCHAR)), SortedRangeSet.all(VARCHAR));
        assertEquals(SortedRangeSet.none(VARCHAR).union(regexLikePatterns(".*123.*")), regexLikePatterns(".*123.*"));
        assertEquals(SortedRangeSet.all(VARCHAR).union(regexLikePatterns(".*123.*")), SortedRangeSet.all(VARCHAR));

        assertEquals(regexLikePatterns(".*123.*").intersect(SortedRangeSet.none(VARCHAR)), SortedRangeSet.none(VARCHAR));
        assertEquals(SortedRangeSet.none(VARCHAR).intersect(regexLikePatterns(".*123.*")), SortedRangeSet.none(VARCHAR));
        // Note: the following results are suboptimal.
        assertEquals(regexLikePatterns(".*123.*").intersect(SortedRangeSet.all(VARCHAR)), SortedRangeSet.all(VARCHAR));
        assertEquals(SortedRangeSet.all(VARCHAR).intersect(regexLikePatterns(".*123.*")), SortedRangeSet.all(VARCHAR));

        ValueSet r1 = ValueSet.of(VARCHAR, utf8Slice("a"), utf8Slice("b"));
        ValueSet r2 = ValueSet.of(VARCHAR, utf8Slice("b"), utf8Slice("c"));
        ValueSet s1 = regexLikePatterns("x.*", "y.*");
        ValueSet s2 = regexLikePatterns("y.*", "z.*");

        assertEquals(s1.union(s2), regexLikePatterns("x.*", "y.*", "z.*"));
        assertEquals(s2.union(s1), regexLikePatterns("x.*", "y.*", "z.*"));
        assertEquals(s1.intersect(s2), s2);
        assertEquals(s2.intersect(s1), s1);

        assertEquals(r1.intersect(s1), r1);
        assertEquals(r1.intersect(s2), r1);
        assertEquals(r2.intersect(s1), r2);
        assertEquals(r2.intersect(s2), r2);

        assertEquals(s1.intersect(r1), r1);
        assertEquals(s1.intersect(r2), r2);
        assertEquals(s2.intersect(r1), r1);
        assertEquals(s2.intersect(r2), r2);

        assertEquals(r2.union(s2).intersect(r1), r1);
        assertEquals(r1.union(s1).intersect(r2), r2);
        assertEquals(r1.intersect(r2.union(s2)), r1);
        assertEquals(r2.intersect(r1.union(s1)), r2);

        assertEquals(r1.union(s1).intersect(r2.union(s2)), r1.union(r2).union(s2));
        assertEquals(r2.union(s2).intersect(r1.union(s1)), r1.union(r2).union(s1));
    }

    @Test
    public void testLikeAndRegexpLike()
    {
        SortedRangeSet likeAndRegexpLike = regexLikePatterns(".*123.*").union(likePatterns("%456%"));

        assertEquals(likeAndRegexpLike.getStringMatchers().getRegexpLikePatterns(), Sets.newHashSet(".*123.*"));
        assertEquals(likeAndRegexpLike.getStringMatchers().getLikePatterns(), Sets.newHashSet("%456%"));

        assertEquals(likeAndRegexpLike.union(SortedRangeSet.none(VARCHAR)), likeAndRegexpLike);
        assertEquals(likeAndRegexpLike.union(SortedRangeSet.all(VARCHAR)), SortedRangeSet.all(VARCHAR));
        assertEquals(SortedRangeSet.none(VARCHAR).union(likeAndRegexpLike), likeAndRegexpLike);
        assertEquals(SortedRangeSet.all(VARCHAR).union(likeAndRegexpLike), SortedRangeSet.all(VARCHAR));

        assertEquals(likeAndRegexpLike.intersect(SortedRangeSet.none(VARCHAR)), SortedRangeSet.none(VARCHAR));
        assertEquals(SortedRangeSet.none(VARCHAR).intersect(likeAndRegexpLike), SortedRangeSet.none(VARCHAR));
        // Note: the following results are suboptimal.
        assertEquals(likeAndRegexpLike.intersect(SortedRangeSet.all(VARCHAR)), SortedRangeSet.all(VARCHAR));
        assertEquals(SortedRangeSet.all(VARCHAR).intersect(likeAndRegexpLike), SortedRangeSet.all(VARCHAR));

        ValueSet r1 = ValueSet.of(VARCHAR, utf8Slice("a"), utf8Slice("b"));
        ValueSet r2 = ValueSet.of(VARCHAR, utf8Slice("b"), utf8Slice("c"));
        ValueSet s1 = regexLikePatterns(".*x.*", ".*y.*").union(likePatterns("%x%", "%y%"));
        ValueSet s2 = regexLikePatterns(".*y.*", ".*z.*").union(likePatterns("%y%", "%z%"));
        ValueSet expectedS1AndS2Union = regexLikePatterns(".*x.*", ".*y.*", ".*z.*").union(likePatterns("%x%", "%y%", "%z%"));

        assertEquals(s1.union(s2), expectedS1AndS2Union);
        assertEquals(s2.union(s1), expectedS1AndS2Union);
        assertEquals(s1.intersect(s2), s2);
        assertEquals(s2.intersect(s1), s1);

        assertEquals(r2.intersect(s1), r2);
        assertEquals(r2.intersect(s2), r2);

        assertEquals(s1.intersect(r1), r1);
        assertEquals(s1.intersect(r2), r2);
        assertEquals(s2.intersect(r1), r1);
        assertEquals(s2.intersect(r2), r2);

        assertEquals(r2.union(s2).intersect(r1), r1);
        assertEquals(r1.union(s1).intersect(r2), r2);
        assertEquals(r1.intersect(r2.union(s2)), r1);
        assertEquals(r2.intersect(r1.union(s1)), r2);

        assertEquals(r1.union(s1).intersect(r2.union(s2)), r1.union(r2).union(s2));
        assertEquals(r2.union(s2).intersect(r1.union(s1)), r1.union(r2).union(s1));
    }

    private SortedRangeSet likePatterns(SortedRangeSet sortedRangeSet, String... patterns)
    {
        SortedRangeSet result = sortedRangeSet;
        for (String pattern : patterns) {
            result = result.union(SortedRangeSet.ofLike(VARCHAR, pattern));
        }
        return result;
    }

    private SortedRangeSet likePatterns(String... patterns)
    {
        return likePatterns(SortedRangeSet.none(VARCHAR), patterns);
    }

    private SortedRangeSet regexLikePatterns(SortedRangeSet sortedRangeSet, String... patterns)
    {
        SortedRangeSet result = sortedRangeSet;
        for (String pattern : patterns) {
            result = result.union(SortedRangeSet.ofRegexpLike(VARCHAR, pattern));
        }
        return result;
    }

    private SortedRangeSet regexLikePatterns(String... patterns)
    {
        return regexLikePatterns(SortedRangeSet.none(VARCHAR), patterns);
    }

    private void assertUnion(SortedRangeSet first, SortedRangeSet second, SortedRangeSet expected)
    {
        assertEquals(first.union(second), expected);
        assertEquals(first.union(ImmutableList.of(first, second)), expected);
    }

    private static SortedRangeSetAssert assertSortedRangeSet(SortedRangeSet sortedRangeSet)
    {
        return assertThat((AssertProvider<SortedRangeSetAssert>) () -> new SortedRangeSetAssert(sortedRangeSet));
    }

    private static class SortedRangeSetAssert
    {
        private final SortedRangeSet sortedRangeSet;

        public SortedRangeSetAssert(SortedRangeSet sortedRangeSet)
        {
            this.sortedRangeSet = requireNonNull(sortedRangeSet, "sortedRangeSet is null");
        }

        public SortedRangeSetAssert containsValue(Object value)
        {
            if (!sortedRangeSet.containsValue(value)) {
                throw new AssertionError(format("Expected %s to contain %s", sortedRangeSet, value));
            }
            return this;
        }

        public SortedRangeSetAssert doesNotContainValue(Object value)
        {
            if (sortedRangeSet.containsValue(value)) {
                throw new AssertionError(format("Expected %s not to contain %s", sortedRangeSet, value));
            }
            return this;
        }
    }
}
