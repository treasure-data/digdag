package io.digdag.standards.operator.bq;

import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.digdag.client.config.ConfigException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class Bq
{
    private static final Pattern TABLE_REFERENCE_PATTERN = Pattern.compile("^(?:(?<project>[^:]+):)?(?:(?<dataset>[^.]+)\\.)?(?<table>[a-zA-Z0-9_]{1,1024})$");

    @VisibleForTesting
    static TableReference tableReference(String defaultProjectId, Optional<String> defaultDataset, String s)
    {
        Matcher matcher = TABLE_REFERENCE_PATTERN.matcher(s);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Bad table reference: " + s);
        }

        String project = Optional.fromNullable(matcher.group("project")).or(defaultProjectId);
        Optional<String> dataset = Optional.fromNullable(matcher.group("dataset")).or(defaultDataset);
        String table = matcher.group("table");

        if (!dataset.isPresent()) {
            throw new IllegalArgumentException("Bad table reference. Either configure 'default_dataset' or include dataset name in table reference: " + s);
        }

        return new TableReference()
                .setProjectId(project)
                .setDatasetId(dataset.get())
                .setTableId(table);
    }

    private static final Pattern DATASET_REFERENCE_PATTERN = Pattern.compile("^(?:(?<project>[^:]+):)?(?<dataset>[^.]+)$");

    @VisibleForTesting
    static DatasetReference datasetReference(String s)
    {
        Matcher matcher = DATASET_REFERENCE_PATTERN.matcher(s);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Bad dataset reference: " + s);
        }
        return new DatasetReference()
                .setProjectId(matcher.group("project"))
                .setDatasetId(matcher.group("dataset"));
    }
}
