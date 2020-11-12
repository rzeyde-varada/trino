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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestMetrics
{
    @Test
    public void testMerge()
    {
        Map<String, Metric> m1 = ImmutableMap.of(
                "a", new Count(1),
                "b", new Count(2));
        Map<String, Metric> m2 = ImmutableMap.of(
                "b", new Count(3),
                "c", new Count(4));
        Metrics merged = Metrics.merge(ImmutableList.of(m1, m2));
        assertEquals(merged.get(), ImmutableMap.of(
                "a", new Count(1),
                "b", new Count(5),
                "c", new Count(4)));
    }
}
