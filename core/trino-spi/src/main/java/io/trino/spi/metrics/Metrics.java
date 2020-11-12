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

import io.trino.spi.Mergeable;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class Metrics
{
    public static final Metrics EMPTY = new Metrics(Map.of());

    private final Map<String, Metric> map;

    public Metrics(Map<String, Metric> map)
    {
        this.map = Map.copyOf(requireNonNull(map, "map is null"));
    }

    public static Metrics merge(Iterable<Map<String, Metric>> maps)
    {
        Map<String, Metric> merged = new HashMap();
        maps.forEach(map -> map
                .entrySet()
                .stream()
                .forEach(entry -> merged.merge(
                        entry.getKey(),
                        entry.getValue(),
                        (left, right) -> mergeInternal(left, right))));
        return new Metrics(merged);
    }

    @SuppressWarnings("unchecked")
    private static <T> Metric mergeInternal(Metric left, Metric right)
    {
        return (Metric) ((Mergeable<T>) left).mergeWith((T) right);
    }

    public Map<String, Metric> get()
    {
        return map;
    }
}
