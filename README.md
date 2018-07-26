# Digdag

[![Circle CI](https://circleci.com/gh/treasure-data/digdag.svg?style=svg&circle-token=8ccc5c665022ce4d1ee05cf7b829c84877387a6c)](https://circleci.com/gh/treasure-data/digdag)

[![Travis CI](https://travis-ci.org/treasure-data/digdag.svg?branch=master)](https://travis-ci.org/treasure-data/digdag)

## [Documentation](http://digdag.io)

Please check [digdag.io](http://digdag.io) for installation & user manual.

## Development

### Prerequirements

* JDK 8
* Node.js 7.x

Installing Node.js using nodebrew:

```
$ curl -L git.io/nodebrew | perl - setup
$ echo 'export PATH=$HOME/.nodebrew/current/bin:$PATH' >> ~/.bashrc
$ source ~/.bashrc
$ nodebrew install-binary v7.x
$ nodebrew use v7.x
```

Installing Node.js using Homebrew on Mac OS X:

```
$ brew install node
```

* Python 3
  * sphinx
  * sphinx_rtd_theme
  * recommonmark

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

## Building CLI executables

```
$ ./gradlew cli
$ ./gradlew cli -PwithoutUi  # build without integrated UI
```

(If the command fails during building UI due to errors from `node` command, you can try to add `-PwithoutUi` argument to exclude the UI from the package).

It makes an executable in `pkg/`, e.g. `pkg/digdag-$VERSION.jar`.

### Releasing a new version

You need to set Bintray user name and API key in `BINTRAY_USER` and `BINTRAY_KEY` environment variables.

1. run `git pull origin --tags`.
2. run `./gradlew setVersion -Pto=<version>` command.
3. write release notes to `releases/release-<version>.rst` file. It must include at least version (the first line) and release date (the last line).
4. run `./gradlew clean cli site check releaseCheck`.
5. if it succeeded, run `./gradlew release`.

If major version is incremented, also update `version =` and `release =` at [digdag-docs/src/conf.py](digdag-docs/src/conf.py).


### Releasing a SNAPSHOT version

```
./gradlew releaseSnapshot
```


### Develop digdag-ui

Node.js development server is useful because it reloads changes of digdag-ui source code automatically.

First, put following lines to ~/.config/digdag/config and start digdag server:

```
server.http.headers.access-control-allow-origin = http://localhost:9000
server.http.headers.access-control-allow-headers = origin, content-type, accept, authorization, x-td-account-override, x-xsrf-token, cookie
server.http.headers.access-control-allow-credentials = true
server.http.headers.access-control-allow-methods = GET, POST, PUT, DELETE, OPTIONS, HEAD
server.http.headers.access-control-max-age = 1209600
```

Then, start digdag-ui development server:

```
$ cd digdag-ui/
$ npm install
$ npm run dev    # starts dev server on http://localhost:9000/
```


### Updating documents

Documents are in digdag-docs/src directory. They're built using Sphinx.

Website is hosted on [www.digdag.io](http://www.digdag.io) using Github Pages. Pages are built using deployment step of circle.yml and automatically pushed to [gh-pages branch of digdag-docs repository](https://github.com/treasure-data/digdag-docs/tree/gh-pages).

To build the pages and check them locally, run following command:

```
$ ./gradlew site
```

This might not always update all necessary files (Sphinx doesn't manage update dependencies well). In this case, run `./gradlew clean` first.

It builds index.html at digdag-docs/build/html/index.html.

### Release Notes

The list of release note is [here](https://github.com/treasure-data/digdag/tree/master/digdag-docs/src/releases).


