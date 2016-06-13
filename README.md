# digdag

[![Circle CI](https://circleci.com/gh/treasure-data/digdag.svg?style=svg&circle-token=8ccc5c665022ce4d1ee05cf7b829c84877387a6c)](https://circleci.com/gh/treasure-data/digdag)

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
5. if it succeeded, run `./gradlew release`.

If major version is incremented, update `version =` and `release =` at [digdag-docs/src/conf.py](digdag-docs/src/conf.py).


### Updating documents

Documents are in digdag-docs/src directory. They're built using Sphinx.

Website is hosted on [www.digdag.io](http://www.digdag.io) using Github Pages. Pages are built using deployment step of circle.yml and automatically pushed to [gh-pages branch of digdag-docs repository](https://github.com/treasure-data/digdag-docs/tree/gh-pages).

To build the pages and check them locally, run following command:

```
$ ./gradlew site
```

This might not always update all necessary files (Sphinx doesn't manage update dependencies well). In this case, run `./gradlew clean` first.

It buids index.html at digdag-docs/build/html/index.html.

