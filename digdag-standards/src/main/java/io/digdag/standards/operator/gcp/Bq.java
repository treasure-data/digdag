package io.digdag.standards.operator.gcp;

import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.common.base.Optional;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class Bq
{
    private static final Pattern TABLE_REFERENCE_PATTERN = Pattern.compile(
            "^(?:(?<project>[^:]+):)?(?:(?<dataset>[^.]+)\\.)?(?<table>[a-zA-Z0-9_]{1,1024}(?:\\$[0-9]{4,10})?)$");

    static TableReference tableReference(String defaultProjectId, Optional<DatasetReference> defaultDataset, String s)
    {
        Matcher matcher = TABLE_REFERENCE_PATTERN.matcher(s);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Bad table reference: " + s);
        }

        String project = matcher.group("project");
        if (project == null) {
            if (defaultDataset.isPresent() && defaultDataset.get().getProjectId() != null) {
                project = defaultDataset.get().getProjectId();
            }
            else {
                project = defaultProjectId;
            }
        }

        Optional<String> dataset = Optional.fromNullable(matcher.group("dataset"))
                .or(defaultDataset.transform(DatasetReference::getDatasetId));

        String table = matcher.group("table");

        if (!dataset.isPresent()) {
            throw new IllegalArgumentException("Bad table reference. Either configure 'dataset' or include dataset name in table reference: " + s);
        }

        return new TableReference()
                .setProjectId(project)
                .setDatasetId(dataset.get())
                .setTableId(table);
    }

    private static final Pattern DATASET_REFERENCE_PATTERN = Pattern.compile("^(?:(?<project>[^:]+):)?(?<dataset>[^.]+)$");

    static DatasetReference datasetReference(String s)
    {
        return datasetReference(Optional.absent(), s);
    }

    static DatasetReference datasetReference(String defaultProjectId, String s)
    {
        return datasetReference(Optional.of(defaultProjectId), s);
    }

    static DatasetReference datasetReference(Optional<String> defaultProjectId, String s)
    {
        Matcher matcher = DATASET_REFERENCE_PATTERN.matcher(s);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Bad dataset reference: " + s);
        }
        return new DatasetReference()
                .setProjectId(Optional.fromNullable(matcher.group("project")).or(defaultProjectId).orNull())
                .setDatasetId(matcher.group("dataset"));
    }
}
