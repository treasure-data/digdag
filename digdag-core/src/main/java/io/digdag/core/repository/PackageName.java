package io.digdag.core.repository;

import com.google.common.base.Optional;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public class PackageName
{
    @JsonCreator
    public static PackageName of(String fullName)
    {
        return new PackageName(fullName);
    }

    public static PackageName root()
    {
        return new PackageName("");
    }

    private final String fullName;

    private PackageName(String fullName)
    {
        this.fullName = fullName;
    }

    @JsonValue
    public String getFullName()
    {
        return fullName;
    }

    public boolean isRoot()
    {
        return fullName.isEmpty();
    }

    public PackageName resolve(String subPackage)
    {
        return PackageName.of(fullName + "/" + subPackage);
    }

    public PackageName resolve(Optional<String> subPackage)
    {
        if (subPackage.isPresent()) {
            return resolve(subPackage.get());
        }
        else {
            return this;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof PackageName)) {
            return false;
        }
        PackageName obj = (PackageName) o;
        return fullName.equals(obj.fullName);
    }

    @Override
    public int hashCode()
    {
        return fullName.hashCode();
    }

    @Override
    public String toString()
    {
        return fullName;
    }
}
