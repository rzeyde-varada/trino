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

package io.trino.spi.metrics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.stats.TDigest;
import io.trino.spi.Mergeable;

import java.util.Arrays;

public class Histogram
        implements Metric, Mergeable<Histogram>
{
    TDigest digest;
    // limits must be sorted.
    private final double[] limits;
    // buckets[i] is the number of events with value between limit[i-1] and limits[i], with limits[-1] being "-infinity".
    private final long[] buckets;
    private final long count;
    private final double min;
    private final double max;
    private final double sum;

    @JsonCreator
    public Histogram(double[] limits, long[] buckets, long count, double min, double max, double sum)
    {
        this.limits = limits;
        this.buckets = buckets;
        this.count = count;
        this.min = min;
        this.max = max;
        this.sum = sum;
    }

    @JsonProperty("limits")
    public double[] getLimits()
    {
        return limits;
    }

    @JsonProperty("buckets")
    public long[] getBuckets()
    {
        return buckets;
    }

    @JsonProperty("count")
    public long getCount()
    {
        return count;
    }

    @JsonProperty("min")
    public double getMin()
    {
        return min;
    }

    @JsonProperty("max")
    public double getMax()
    {
        return max;
    }

    @JsonProperty("sum")
    public double getSum()
    {
        return sum;
    }

    @Override
    public Histogram mergeWith(Histogram other)
    {
        if (!Arrays.equals(limits, other.limits)) {
            throw new IllegalArgumentException("histograms must have same limits to be merged");
        }
        long[] merged = new long[limits.length];
        for (int i = 0; i < limits.length; i++) {
            merged[i] = buckets[i] + other.buckets[i];
        }
        return new Histogram(
                limits,
                merged,
                count + other.count,
                Math.min(min, other.min),
                Math.max(max, other.max),
                sum + other.sum);
    }

    public static Builder builder(double[] limits)
    {
        return new Builder(limits);
    }

    public static class Builder
    {
        private final double[] limits;
        private final long[] buckets;
        private long count;
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;

        private Builder(double[] limits)
        {
            Arrays.sort(limits);
            this.limits = limits;
            this.buckets = new long[limits.length];
            this.count = 0;
        }

        public Builder add(double... values)
        {
            for (double value : values) {
                if (count == 0) {
                    min = value;
                    max = value;
                }
                else {
                    min = Math.min(min, value);
                    max = Math.max(max, value);
                }
                count += 1;
                sum += value;
                for (int i = 0; i < limits.length; ++i) {
                    if (value <= limits[i]) {
                        buckets[i] += 1;
                        break;
                    }
                }
            }
            return this;
        }

        public Histogram build()
        {
            return new Histogram(limits, buckets, count, min, max, sum);
        }
    }
}
