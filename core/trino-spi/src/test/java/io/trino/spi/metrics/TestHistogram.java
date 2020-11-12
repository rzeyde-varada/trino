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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestHistogram
{
    @Test
    void testBuild()
    {
        double[] limits = new double[] {0.1, 1, 10};
        Histogram histogram = Histogram
                .builder(limits)
                .add(0.5, 0, 2, 10, 1234)
                .add(0.25, 20, 4)
                .build();
        assertEquals(histogram.getBuckets(), new long[] {1, 2, 3});
        assertEquals(histogram.getLimits(), limits);
        assertEquals(histogram.getCount(), 8);
        assertEquals(histogram.getMin(), 0.0);
        assertEquals(histogram.getMax(), 1234.0);
        assertEquals(histogram.getSum(), 1270.75);
    }

    @Test
    void testMerge()
    {
        double[] limits = new double[] {0.1, 1, 10};
        Histogram h1 = Histogram.builder(limits).add(2).build();
        Histogram h2 = Histogram.builder(limits).add(0.5).build();
        Histogram merged = h1.mergeWith(h2);
        assertEquals(merged.getBuckets(), new long[] {0, 1, 1});
        assertEquals(merged.getLimits(), limits);
        assertEquals(merged.getCount(), 2);
        assertEquals(merged.getMin(), 0.5);
        assertEquals(merged.getMax(), 2.0);
        assertEquals(merged.getSum(), 2.5);
    }

    @Test
    void testEmpty()
    {
        double[] limits = new double[] {0.1, 1, 10};
        Histogram histogram = Histogram
                .builder(limits)
                .build();
        assertEquals(histogram.getBuckets(), new long[] {0, 0, 0});
        assertEquals(histogram.getLimits(), limits);
        assertEquals(histogram.getCount(), 0);
        assertEquals(histogram.getMin(), Double.POSITIVE_INFINITY);
        assertEquals(histogram.getMax(), Double.NEGATIVE_INFINITY);
        assertEquals(histogram.getSum(), 0.0);
    }

    @Test
    void testNoLimits()
    {
        double[] limits = new double[0];
        Histogram histogram = Histogram
                .builder(limits)
                .add(1, 2, 3)
                .build();
        assertEquals(histogram.getBuckets(), new long[0]);
        assertEquals(histogram.getLimits(), limits);
        assertEquals(histogram.getCount(), 3);
        assertEquals(histogram.getMin(), 1.0);
        assertEquals(histogram.getMax(), 3.0);
        assertEquals(histogram.getSum(), 6.0);
    }
}
