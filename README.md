# digdag

# How to run examples

## Using Ruby PoC version:

1. install Ruby >= 2.1
2. install necessary gems: $ gem install sigdump fluentd concurrent serverengine uuidtools liquid
3. (optional) install graphviz: $ brew install graphviz
4. run $ ./bin/digdag examples/NAME.yml
5. (optional) visualize task dependency: $ dot -Tpng graph.dot -o graph.png

## Using Java version:

1. install Java JDK >= 8
2. build command: $ ./gradlew cli
3. install graphviz: $ brew install graphviz
4. run $ ./pkg/digdag-0.1.0.jar run examples/NAME.yml

