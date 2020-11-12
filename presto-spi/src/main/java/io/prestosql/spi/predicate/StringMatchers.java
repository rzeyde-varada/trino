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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StringMatchers
{
    // likePatterns and regexpLikePatterns are semantically OR-ed together
    private final Set<String> likePatterns;
    private final Set<String> regexpLikePatterns;

    private StringMatchers(Set<String> likePatterns, Set<String> regexpLikePatterns)
    {
        this.likePatterns = likePatterns;
        this.regexpLikePatterns = regexpLikePatterns;
    }

    static Builder builder()
    {
        return new Builder();
    }

    static StringMatchers empty()
    {
        return builder().build();
    }

    @JsonCreator
    public static StringMatchers copyOf(
            @JsonProperty("likePatterns") Set<String> likePatterns,
            @JsonProperty("regexpLikePatterns") Set<String> regexpLikePatterns)
    {
        return builder()
                .addLikePatterns(likePatterns)
                .addRegexpLikePatterns(regexpLikePatterns)
                .build();
    }

    public boolean isEmpty()
    {
        return likePatterns.isEmpty() && regexpLikePatterns.isEmpty();
    }

    public StringMatchers intersect(StringMatchers other)
    {
        // Likes1 * Likes2 <= min(Likes1, Likes2) -> choose the "shorter" one.
        // It would allow optimizing `col = 'a' AND col LIKE '%z'` into `col = 'a'`.
        if (this.likePatterns.size() + this.regexpLikePatterns.size() < other.likePatterns.size() + other.regexpLikePatterns.size()) {
            return new StringMatchers(this.likePatterns, this.regexpLikePatterns);
        }
        else {
            return new StringMatchers(other.likePatterns, other.regexpLikePatterns);
        }
    }

    @JsonProperty("likePatterns")
    public Set<String> getLikePatterns()
    {
        return new HashSet<>(likePatterns);
    }

    @JsonProperty("regexpLikePatterns")
    public Set<String> getRegexpLikePatterns()
    {
        return new HashSet<>(regexpLikePatterns);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StringMatchers that = (StringMatchers) o;
        return Objects.equals(likePatterns, that.likePatterns) &&
                Objects.equals(regexpLikePatterns, that.regexpLikePatterns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(likePatterns, regexpLikePatterns);
    }

    @Override
    public String toString()
    {
        return Stream.concat(likePatterns.stream().map(p -> "LIKE(" + p + ")"),
                             regexpLikePatterns.stream().map(p -> "REGEXP(" + p + ")"))
                     .collect(Collectors.joining(", "));
    }

    static class Builder
    {
        private final Set<String> likePatterns = new HashSet<>();
        private final Set<String> regexpLikePatterns = new HashSet<>();

        private Builder()
        {
        }

        Builder add(StringMatchers stringMatchers)
        {
            addLikePatterns(stringMatchers.likePatterns);
            addRegexpLikePatterns(stringMatchers.regexpLikePatterns);
            return this;
        }

        Builder addLikePatterns(String... likePatterns)
        {
            addLikePatterns(Arrays.asList(likePatterns));
            return this;
        }

        Builder addLikePatterns(Collection<String> likePatterns)
        {
            this.likePatterns.addAll(likePatterns);
            return this;
        }

        Builder addRegexpLikePatterns(String... regexpLikePatterns)
        {
            addRegexpLikePatterns(Arrays.asList(regexpLikePatterns));
            return this;
        }

        Builder addRegexpLikePatterns(Collection<String> regexpLikePatterns)
        {
            this.regexpLikePatterns.addAll(regexpLikePatterns);
            return this;
        }

        StringMatchers build()
        {
            return new StringMatchers(likePatterns, regexpLikePatterns);
        }
    }
}
