# digdag

## [Documentation](http://digdag.io)

Please check [digdag.io](http://digdag.io) for installation & user manual.

## Development

### Running tests

```
$ ./gradlew check
```

Test coverage report is generated at `didgag-*/build/reports/jacoco/test/html/index.html`.
Findbugs report is generated at `digdag-*/build/reports/findbugs/main.html`.

### Testing with PostgreSQL

Test uses in-memory H2 database by default. To use PostgreSQL, set following environment variables:

```
$ export DIGDAG_TEST_POSTGRESQL="$(cat config/test_postgresql.properties)"
```

### Releasing a new version

1. run `./gradlew setVersion -Pto=<version>` command.
2. add `releases/release-<version>` line to [digdag-docs/src/releases.rst](digdag-docs/src/releases.rst) (setVersion should do this automatically but not implemented yet).
3. write release notes to `releases/release-<version>.rst` file. It must include at least version (the first line) and release date (the last line).
4. run `./gradlew clean cli check releaseCheck`.
5. if it succeeded, run `./gradlew release docsUpload`.

If major version is incremented, update `version =` and `release =` at [digdag-docs/src/conf.py](digdag-docs/src/conf.py) and run `./gradlew :digdag-docs:clean docsUpload`.


### Updating documents

Documents are in digdag-docs/src. They're built using Sphinx.

Website is hosted on [digdag.io](http://digdag.io) using Amazon S3.

To build the documents and check HTML locally, run following command:

```
$ ./gradlew sphinxHtml
```

It buids index.html at digdag-docs/build/html/index.html.

If it's good, upload the documents to the website using following command:

```
$ ./gradlew docsUpload
```

This might not always update all necessary files (Sphinx doesn't manage update dependencies well). In this case, run `./gradlew clean` first.

