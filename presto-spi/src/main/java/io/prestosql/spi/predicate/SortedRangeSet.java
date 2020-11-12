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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.TypeUtils.isFloatingPointNaN;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * A set containing zero or more Ranges of the same type over a continuous space of possible values.
 * Ranges are coalesced into the most compact representation of non-overlapping Ranges. This structure
 * allows iteration across these compacted Ranges in increasing order, as well as other common
 * set-related operation.
 */
public final class SortedRangeSet
        implements ValueSet
{
    private final Type type;
    private final NavigableMap<Marker, Range> lowIndexedRanges;

    // HACK: side-channel for passing LIKE patterns to connectors as an optimization (i.e. "hint").
    // Semantically, it's OR-ed together with the existing ranges of this class.
    private final StringMatchers stringMatchers;

    private SortedRangeSet(Type type, NavigableMap<Marker, Range> lowIndexedRanges, StringMatchers stringMatchers)
    {
        requireNonNull(type, "type is null");
        requireNonNull(lowIndexedRanges, "lowIndexedRanges is null");

        if (!type.isOrderable()) {
            throw new IllegalArgumentException("Type is not orderable: " + type);
        }
        this.type = type;
        this.lowIndexedRanges = lowIndexedRanges;
        this.stringMatchers = isAll() ? StringMatchers.empty() : stringMatchers;
    }

    static SortedRangeSet none(Type type)
    {
        return copyOf(type, Collections.emptyList());
    }

    static SortedRangeSet all(Type type)
    {
        return copyOf(type, Collections.singletonList(Range.all(type)));
    }

    /**
     * Provided discrete values that are unioned together to form the SortedRangeSet
     */
    static SortedRangeSet of(Type type, Object first, Object... rest)
    {
        List<Range> ranges = new ArrayList<>(rest.length + 1);
        ranges.add(Range.equal(type, first));
        for (Object value : rest) {
            ranges.add(Range.equal(type, value));
        }
        return copyOf(type, ranges);
    }

    /**
     * Provided Ranges are unioned together to form the SortedRangeSet
     */
    static SortedRangeSet of(Range first, Range... rest)
    {
        List<Range> rangeList = new ArrayList<>(rest.length + 1);
        rangeList.add(first);
        rangeList.addAll(asList(rest));
        return copyOf(first.getType(), rangeList);
    }

    /**
     * Provided Ranges are unioned together to form the SortedRangeSet
     */
    static SortedRangeSet copyOf(Type type, Iterable<Range> ranges, StringMatchers stringMatchers)
    {
        return new Builder(type).addAll(ranges)
                .addStringMatchers(stringMatchers)
                .build();
    }

    static SortedRangeSet copyOf(Type type, Iterable<Range> ranges)
    {
        return new Builder(type).addAll(ranges).build();
    }

    static SortedRangeSet ofLike(Type type, String likePattern)
    {
        // HACK: side-channel for passing LIKE patterns to connectors.
        // It has no ranges, so it's almost a "NONE", except the LIKE and REGEXP LIKE patterns from DomainTranslator#fromPredicate.
        // TODO: if this pattern cannot be optimized, it should be handled as ALL (i.e. full scan).
        StringMatchers stringMatchers = StringMatchers.builder().addLikePatterns(likePattern).build();
        return new Builder(type).addStringMatchers(stringMatchers).build();
    }

    static SortedRangeSet ofRegexpLike(Type type, String regexpLikePattern)
    {
        // HACK: side-channel for passing REGEXP LIKE patterns to connectors.
        // It has no ranges, so it's almost a "NONE", except the LIKE and REGEXP LIKE patterns from DomainTranslator#fromPredicate.
        StringMatchers stringMatchers = StringMatchers.builder().addRegexpLikePatterns(regexpLikePattern).build();
        return new Builder(type).addStringMatchers(stringMatchers).build();
    }

    @JsonCreator
    public static SortedRangeSet copyOf(
            @JsonProperty("type") Type type,
            @JsonProperty("ranges") List<Range> ranges,
            @JsonProperty("stringMatchers") StringMatchers stringMatchers)
    {
        return copyOf(type, (Iterable<Range>) ranges, stringMatchers);
    }

    public static SortedRangeSet copyOf(Type type, List<Range> ranges)
    {
        return copyOf(type, (Iterable<Range>) ranges, StringMatchers.empty());
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty("ranges")
    public List<Range> getOrderedRanges()
    {
        return new ArrayList<>(lowIndexedRanges.values());
    }

    @JsonProperty("stringMatchers")
    public StringMatchers getStringMatchers()
    {
        return StringMatchers.builder().add(stringMatchers).build();
    }

    public int getRangeCount()
    {
        return lowIndexedRanges.size();
    }

    @Override
    public boolean isNone()
    {
        return lowIndexedRanges.isEmpty() && stringMatchers.isEmpty();
    }

    @Override
    public boolean isAll()
    {
        return lowIndexedRanges.size() == 1 && lowIndexedRanges.values().iterator().next().isAll();
    }

    @Override
    public boolean isSingleValue()
    {
        return stringMatchers.isEmpty() && lowIndexedRanges.size() == 1 && lowIndexedRanges.values().iterator().next().isSingleValue();
    }

    @Override
    public Object getSingleValue()
    {
        if (!isSingleValue()) {
            throw new IllegalStateException("SortedRangeSet does not have just a single value");
        }
        return lowIndexedRanges.values().iterator().next().getSingleValue();
    }

    @Override
    public boolean isDiscreteSet()
    {
        for (Range range : lowIndexedRanges.values()) {
            if (!range.isSingleValue()) {
                return false;
            }
        }
        return !isNone();
    }

    @Override
    public List<Object> getDiscreteSet()
    {
        if (!isDiscreteSet()) {
            throw new IllegalStateException("SortedRangeSet is not a discrete set");
        }
        return lowIndexedRanges.values().stream()
                .map(Range::getSingleValue)
                .collect(toUnmodifiableList());
    }

    @Override
    public boolean containsValue(Object value)
    {
        if (isFloatingPointNaN(type, value)) {
            return isAll();
        }
        return includesMarker(Marker.exactly(type, value));
    }

    boolean includesMarker(Marker marker)
    {
        requireNonNull(marker, "marker is null");
        checkTypeCompatibility(marker);

        Map.Entry<Marker, Range> floorEntry = lowIndexedRanges.floorEntry(marker);
        return floorEntry != null && floorEntry.getValue().includes(marker);
    }

    public Range getSpan()
    {
        if (lowIndexedRanges.isEmpty()) {
            throw new IllegalStateException("Cannot get span if no ranges exist");
        }
        return lowIndexedRanges.firstEntry().getValue().span(lowIndexedRanges.lastEntry().getValue());
    }

    @Override
    public Ranges getRanges()
    {
        return new Ranges()
        {
            @Override
            public int getRangeCount()
            {
                return SortedRangeSet.this.getRangeCount();
            }

            @Override
            public List<Range> getOrderedRanges()
            {
                return SortedRangeSet.this.getOrderedRanges();
            }

            @Override
            public Range getSpan()
            {
                return SortedRangeSet.this.getSpan();
            }
        };
    }

    @Override
    public ValuesProcessor getValuesProcessor()
    {
        return new ValuesProcessor()
        {
            @Override
            public <T> T transform(Function<Ranges, T> rangesFunction, Function<DiscreteValues, T> valuesFunction, Function<AllOrNone, T> allOrNoneFunction)
            {
                return rangesFunction.apply(getRanges());
            }

            @Override
            public void consume(Consumer<Ranges> rangesConsumer, Consumer<DiscreteValues> valuesConsumer, Consumer<AllOrNone> allOrNoneConsumer)
            {
                rangesConsumer.accept(getRanges());
            }
        };
    }

    @Override
    public SortedRangeSet intersect(ValueSet other)
    {
        SortedRangeSet otherRangeSet = checkCompatibility(other);

        Builder builder = new Builder(type);

        Iterator<Range> iterator1 = lowIndexedRanges.values().iterator();
        Iterator<Range> iterator2 = otherRangeSet.lowIndexedRanges.values().iterator();

        // this = (Ranges1 + Likes1), other = (Ranges2 + Likes2)
        // + -> UNION
        // * -> INTERSECTION

        // this * other =   Ranges1*Ranges2 + Ranges1*Likes2 + Likes1*Ranges2 + Likes1*Likes2
        if (iterator1.hasNext() && iterator2.hasNext()) {
            Range range1 = iterator1.next();
            Range range2 = iterator2.next();

            while (true) {
                if (range1.overlaps(range2)) {
                    builder.add(range1.intersect(range2));
                }

                if (range1.getHigh().compareTo(range2.getHigh()) <= 0) {
                    if (!iterator1.hasNext()) {
                        break;
                    }
                    range1 = iterator1.next();
                }
                else {
                    if (!iterator2.hasNext()) {
                        break;
                    }
                    range2 = iterator2.next();
                }
            }
        }

        // Likes1 * Ranges2 <= Ranges2 (if Likes1 non-empty)
        if (!this.getStringMatchers().isEmpty()) {
            builder.addAll(otherRangeSet.getOrderedRanges());
        }

        // Likes2 * Ranges1 <= Ranges1 (if Likes2 non-empty)
        if (!otherRangeSet.getStringMatchers().isEmpty()) {
            builder.addAll(this.getOrderedRanges());
        }

        builder.addStringMatchers(this.stringMatchers.intersect(otherRangeSet.stringMatchers));

        return builder.build();
    }

    @Override
    public boolean overlaps(ValueSet other)
    {
        SortedRangeSet otherRangeSet = checkCompatibility(other);

        Iterator<Range> iterator1 = lowIndexedRanges.values().iterator();
        Iterator<Range> iterator2 = otherRangeSet.lowIndexedRanges.values().iterator();

        if (iterator1.hasNext() && iterator2.hasNext()) {
            Range range1 = iterator1.next();
            Range range2 = iterator2.next();

            while (true) {
                if (range1.overlaps(range2)) {
                    return true;
                }

                if (range1.getHigh().compareTo(range2.getHigh()) <= 0) {
                    if (!iterator1.hasNext()) {
                        break;
                    }
                    range1 = iterator1.next();
                }
                else {
                    if (!iterator2.hasNext()) {
                        break;
                    }
                    range2 = iterator2.next();
                }
            }
        }

        return false;
    }

    @Override
    public SortedRangeSet union(ValueSet other)
    {
        SortedRangeSet otherRangeSet = checkCompatibility(other);
        return new Builder(type)
                .addAll(this.lowIndexedRanges.values())
                .addAll(otherRangeSet.lowIndexedRanges.values())
                .addStringMatchers(this.getStringMatchers())
                .addStringMatchers(otherRangeSet.getStringMatchers())
                .build();
    }

    @Override
    public SortedRangeSet union(Collection<ValueSet> valueSets)
    {
        Builder builder = new Builder(type);
        builder.addAll(this.getOrderedRanges())
                .addStringMatchers(this.getStringMatchers());
        for (ValueSet valueSet : valueSets) {
            SortedRangeSet otherRangeSet = checkCompatibility(valueSet);
            builder.addAll(otherRangeSet.getOrderedRanges());
            builder.addStringMatchers(otherRangeSet.getStringMatchers());
        }
        return builder.build();
    }

    @Override
    public SortedRangeSet complement()
    {
        if (!stringMatchers.isEmpty()) {
            throw new AssertionError("Complementing LIKE / REGEXP LIKE patterns is not supported: " + stringMatchers);
        }

        Builder builder = new Builder(type);

        if (lowIndexedRanges.isEmpty()) {
            return builder.add(Range.all(type)).build();
        }

        Iterator<Range> rangeIterator = lowIndexedRanges.values().iterator();

        Range firstRange = rangeIterator.next();
        if (!firstRange.getLow().isLowerUnbounded()) {
            builder.add(new Range(Marker.lowerUnbounded(type), firstRange.getLow().lesserAdjacent()));
        }

        Range previousRange = firstRange;
        while (rangeIterator.hasNext()) {
            Range currentRange = rangeIterator.next();

            Marker lowMarker = previousRange.getHigh().greaterAdjacent();
            Marker highMarker = currentRange.getLow().lesserAdjacent();
            builder.add(new Range(lowMarker, highMarker));

            previousRange = currentRange;
        }

        Range lastRange = previousRange;
        if (!lastRange.getHigh().isUpperUnbounded()) {
            builder.add(new Range(lastRange.getHigh().greaterAdjacent(), Marker.upperUnbounded(type)));
        }

        return builder.build();
    }

    private SortedRangeSet checkCompatibility(ValueSet other)
    {
        if (!getType().equals(other.getType())) {
            throw new IllegalStateException(format("Mismatched types: %s vs %s", getType(), other.getType()));
        }
        if (!(other instanceof SortedRangeSet)) {
            throw new IllegalStateException(format("ValueSet is not a SortedRangeSet: %s", other.getClass()));
        }
        return (SortedRangeSet) other;
    }

    private void checkTypeCompatibility(Marker marker)
    {
        if (!getType().equals(marker.getType())) {
            throw new IllegalStateException(format("Marker of %s does not match SortedRangeSet of %s", marker.getType(), getType()));
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lowIndexedRanges, stringMatchers);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SortedRangeSet other = (SortedRangeSet) obj;
        return Objects.equals(this.lowIndexedRanges, other.lowIndexedRanges)
                && Objects.equals(this.stringMatchers, other.stringMatchers);
    }

    @Override
    public String toString()
    {
        return lowIndexedRanges.values().toString();
    }

    @Override
    public String toString(ConnectorSession session)
    {
        String rangesStr = lowIndexedRanges.values().stream()
                                           .map(range -> range.toString(session))
                                           .collect(Collectors.joining(", "));
        return String.format("[%s, %s]", rangesStr, stringMatchers.toString());
    }

    static class Builder
    {
        private final Type type;
        private final List<Range> ranges = new ArrayList<>();
        private final StringMatchers.Builder stringMatchersBuilder = StringMatchers.builder();

        Builder(Type type)
        {
            requireNonNull(type, "type is null");

            if (!type.isOrderable()) {
                throw new IllegalArgumentException("Type is not orderable: " + type);
            }
            this.type = type;
        }

        Builder add(Range range)
        {
            if (!type.equals(range.getType())) {
                throw new IllegalArgumentException(format("Range type %s does not match builder type %s", range.getType(), type));
            }

            ranges.add(range);
            return this;
        }

        Builder addStringMatchers(StringMatchers stringMatchers)
        {
            if (!stringMatchers.isEmpty()) {
                if (!(type instanceof CharType || type instanceof VarcharType)) {
                    throw new IllegalArgumentException(format("string matching pushdown not supported for %s", type));
                }
                stringMatchersBuilder.add(stringMatchers);
            }
            return this;
        }

        Builder addAll(Iterable<Range> ranges)
        {
            for (Range range : ranges) {
                add(range);
            }
            return this;
        }

        SortedRangeSet build()
        {
            ranges.sort(Comparator.comparing(Range::getLow));

            NavigableMap<Marker, Range> result = new TreeMap<>();

            Range current = null;
            for (Range next : ranges) {
                if (current == null) {
                    current = next;
                    continue;
                }

                if (current.overlaps(next) || current.getHigh().isAdjacent(next.getLow())) {
                    current = current.span(next);
                }
                else {
                    result.put(current.getLow(), current);
                    current = next;
                }
            }

            if (current != null) {
                result.put(current.getLow(), current);
            }

            return new SortedRangeSet(type, result, stringMatchersBuilder.build());
        }
    }
}
