package io.digdag.client;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Version
    implements Comparable<Version>
{
    private final static Pattern DIGDAG_VERSION_PATTERN =
        Pattern.compile("((?:0|[1-9][0-9]*)(?:\\.(?:0|[1-9][0-9]*))*)(?:\\-([a-zA-Z0-9_\\-\\.]+))?");

    private final List<Integer> versionNumbers;
    private final Optional<String> qualifier;

    private Version(List<Integer> versionNumbers, Optional<String> qualifier)
    {
        this.versionNumbers = versionNumbers;
        this.qualifier = qualifier;
    }

    public static Version parse(String versionString)
    {
        Matcher m = DIGDAG_VERSION_PATTERN.matcher(versionString);
        if (!m.matches()) {
            throw new IllegalArgumentException("Invalid version string: " + versionString);
        }

        ImmutableList.Builder<Integer> numbers = ImmutableList.builder();
        for (String number : m.group(1).split("\\.")) {
            try {
                numbers.add(Integer.parseInt(number));
            }
            catch (NumberFormatException ex) {
                throw new IllegalArgumentException("Too big version number: " + versionString, ex);
            }
        }
        Optional<String> qualifier = Optional.fromNullable(m.group(2));

        return new Version(numbers.build(), qualifier);
    }

    public int getMajorVersion()
    {
        return versionNumbers.get(0);
    }

    @Override
    public int compareTo(Version another)
    {
        // 1.0 < 1.1
        // 1.0 = 1.0.0
        int longer = Math.max(versionNumbers.size(), another.versionNumbers.size());
        for (int i = 0; i < longer; i++) {
            int left = versionNumbers.size() > i ? versionNumbers.get(i) : 0;
            int right = another.versionNumbers.size() > i ? another.versionNumbers.get(i) : 0;
            if (left < right) {
                return -1;
            }
            else if (left > right) {
                return 1;
            }
        }

        // 1.0-rc1 < 1.0, 1.0-SNAPSHOT < 1.0
        // if one of them has qualifier, it's older
        if (qualifier.isPresent() && !another.qualifier.isPresent()) {
            return -1;
        }
        else if (!qualifier.isPresent() && another.qualifier.isPresent()) {
            return 1;
        }

        // if both have qualifier, compare with dictionary order
        if (qualifier.isPresent() && another.qualifier.isPresent()) {
            int c = qualifier.get().compareTo(another.qualifier.get());
            if (c < 0) {
                return -1;
            }
            else if (c > 0) {
                return 1;
            }
        }

        return 0;
    }

    public boolean isNewer(Version another)
    {
        return compareTo(another) > 0;
    }

    public boolean isOlder(Version another)
    {
        return compareTo(another) < 0;
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

        Version another = (Version) o;

        return versionNumbers.equals(another.versionNumbers) &&
            qualifier.equals(another.qualifier);
    }

    @Override
    public int hashCode()
    {
        return toString().hashCode();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (int number : versionNumbers) {
            if (first) {
                first = false;
            }
            else {
                sb.append(".");
            }
            sb.append(Integer.toString(number));
        }
        if (qualifier.isPresent()) {
            sb.append("-").append(qualifier.get());
        }
        return sb.toString();
    }
}
