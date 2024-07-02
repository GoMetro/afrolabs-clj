(defproject
  com.gitlab.pieterbreed/components
  "1.0.1"

  :description "Shared clojure components"
  :url "https://github.com/Afrolabs/afrolabs-clj"
  :license {:name "MIT"
            :url "https://github.com/Afrolabs/afrolabs-clj/blob/main/LICENSE"}
  :repositories [["confluent" "https://packages.confluent.io/maven/"]]
  :deploy-repositories [["clojars" {:url           "https://clojars.org/repo"
                                    :sign-releases false}]]
  :dependencies [[org.clojure/clojure "1.11.3"]
                 [com.google.javascript/closure-compiler-unshaded "v20240317"]
                 [org.clojure/clojurescript "1.11.132"]

                 ;; property-based testing, generative testing
                 [org.clojure/test.check "1.1.1"]

                 ;; dependency conflict resolution, also edn reader
                 [org.clojure/tools.reader "1.4.2"]

                 ;; I'm not sure why we're choosing an old version of jetty here.
                 ;; might be cognitect.
                 ;; https://mvnrepository.com/artifact/org.eclipse.jetty/jetty-util
                 [org.eclipse.jetty/jetty-util "9.4.54.v20240208"]
                 [org.eclipse.jetty/jetty-http "9.4.54.v20240208"]
                 [org.eclipse.jetty/jetty-client "9.4.54.v20240208"]

                 ;; conversions between different kinds of casing
                 ;; camelCase -> snake_case -> kebab-case -> PascalCase
                 [camel-snake-kebab "0.4.3"]


                 [metosin/jsonista "0.3.8" :exclusions [com.fasterxml.jackson.core/jackson-core]]

                 ;; monadic utilities for validation, failure and error checking
                 ;; https://github.com/adambard/failjure
                 [failjure "2.3.0"]

                 ;; facilities for asynchronous programming
                 ;; aka golang channels for clojure
                 ;; https://clojure.org/news/2013/06/28/clojure-clore-async-channels
                 [org.clojure/core.async "1.6.681"]

                 ;; https://github.com/cgrand/xforms ; sometimes useful extra transducers - data transformation
                 [net.cgrand/xforms "0.19.6"]

                 ;; https://github.com/redplanetlabs/specter, data transformation DSL
                 [com.rpl/specter "1.1.4"]

                 ;; https://github.com/weavejester/integrant
                 ;; similar to dependency injection
                 [integrant "0.10.0"]

                 ;; aero - a lib for loading configuration (eg integrant config) from edn files
                 [aero "1.1.6"]

                 ;; handling of UNIX process signals like SIGINT
                 [beckon "0.1.1"]

                 ;; https://github.com/http-kit/http-kit
                 ;; http server
                 [http-kit "2.8.0"]

                 ;; password-hashing helpers
                 [buddy/buddy-hashers "2.0.167"]
                 [buddy/buddy-auth "3.0.323"]
                 [buddy/buddy-sign "3.5.351"]

                 [ring/ring-core "1.12.1"]

                 ;; https://github.com/metosin/ring-http-response
                 ;; ring response helper fns
                 [metosin/ring-http-response "0.9.3"]

                 ;; http routing library
                 ;; https://github.com/metosin/reitit
                 [metosin/reitit "0.7.0" :exclusions [com.fasterxml.jackson.core/jackson-core]]

                 ;; https://github.com/dm3/clojure.java-time
                 [clojure.java-time "1.4.2"]
                 [org.threeten/threeten-extra "1.8.0"]

                 ;; https://github.com/clj-commons/iapetos
                 [io.prometheus/simpleclient "0.16.0"]
                 [io.prometheus/simpleclient_hotspot "0.16.0"]
                 [clj-commons/iapetos "0.1.13"]

                 ;; cognitect (custodians of clojure) has a very nice data driven API interface to AWS
                 [com.cognitect.aws/api "0.8.692"]
                 [com.cognitect.aws/endpoints "1.1.12.682"]
                 [com.cognitect.aws/sns "857.2.1574.0"]
                 [com.cognitect.aws/sqs "857.2.1574.0"]
                 [com.cognitect.aws/s3 "868.2.1580.0"]
                 [com.cognitect.aws/monitoring "857.2.1574.0"]
                 [com.cognitect.aws/logs "868.2.1584.0"]

                 ;; NREPL
                 [nrepl "1.1.1"]

                 ;; logging for clj(s) https://github.com/ptaoussanis/timbre
                 [com.taoensso/timbre "6.5.0"]

                 ;; https://github.com/clojure/data.json
                 [org.clojure/data.json "2.5.0"]

                 ;; https://github.com/clojure/data.csv
                 [org.clojure/data.csv "1.1.0"]
                 [com.cnuernber/charred "1.034"]

                 ;; fip, dependency of both malli and shadow-cljs
                 [org.clojure/core.rrb-vector "0.2.0"]
                 [fipp "0.6.26"]

                 ;; malli; schema-driven development and utilities
                 [metosin/malli "0.16.1"]

                 ;; hooking in the result of other java logging frameworks into slf4j
                 [org.slf4j/log4j-over-slf4j "2.0.12"]
                 [org.slf4j/jul-to-slf4j "2.0.12"]
                 [org.slf4j/jcl-over-slf4j "2.0.12"]

                 ;; then routing slf4j to timbre
                 ;; This will allow us to receive, log (and configure) the logs provided by libraries
                 ;; as well as the app logs we create.
                 [com.fzakaria/slf4j-timbre "0.4.1"]

                 ;; possibility to log to json, for use in cloud environments like GCK
                 [viesti/timbre-json-appender "0.2.13"]

                 [com.fasterxml.jackson.core/jackson-databind "2.17.0"]
                 [com.fasterxml.jackson.datatype/jackson-datatype-jsr310 "2.17.0"]


                 ;; https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
                 ;; [org.apache.kafka/kafka-clients "3.6.1"]


                 ;; following dependencies loosely inspired by the confluent example java app
                 ;; https://github.com/confluentinc/examples/blob/6.1.1-post/clients/cloud/java/pom.xml
                 ;; [org.apache.kafka/kafka-streams "3.6.1"]

                 ;; dependency conflict resolution introduced by confluent libraries
                 ;; [com.fasterxml.jackson.datatype/jackson-datatype-jdk8 "2.16.1"]
                 [com.google.re2j/re2j "1.7"]
                 [org.bouncycastle/bcpkix-jdk18on "1.77"]
                 [ch.qos.reload4j/reload4j "1.2.25"]
                 [org.javassist/javassist "3.29.0-GA"]

                 ;; dependency issue resolutions brought on by confluent
                 ;; [jakarta.xml.bind/jakarta.xml.bind-api "2.3.3" :upgrade false]
                 [joda-time "2.12.7"]

                 ;; dependencies for confluent cloud
                 ;; [org.apache.kafka/connect-runtime "3.6.1" :exclusions [org.slf4j/slf4j-log4j12]]
                 ;; [io.confluent/kafka-json-serializer "7.5.3"  :exclusions []]
                 ;; [io.confluent/kafka-json-schema-serializer "7.5.3" :exclusions [org.apache.kafka/kafka-clients
                 ;;                                                                 org.glassfish.jersey.core/jersey-common]]

                 ;; for use with ksqld, the java client
                 [io.confluent.ksql/ksqldb-api-client "7.6.0" :exclusions [org.slf4j/slf4j-log4j12
                                                                           org.bouncycastle/bc-fips
                                                                           org.javassist/javassist]]

                 [org.slf4j/slf4j-api "2.0.12"]
                 ;; [org.slf4j/slf4j-log4j12 "1.7.32"]
                 [io.confluent/confluent-log4j "1.2.17-cp5"]

                 
                 [thheller/shadow-cljs "2.28.8"]

                 ;; middleware that adds "X-Clacks-Overhead" to responses
                 [gsnewmark/ring-pratchett "0.1.0"]

                 ;; rules engine
                 [net.sekao/odoyle-rules "1.3.1"]

                 ;; profiling
                 ;; this is safe to distribute into prod according to the docs
                 [com.clojure-goes-fast/clj-async-profiler "1.2.2"]

                 ;; uri handling
                 [lambdaisland/uri "1.19.155"]

                 ;; html as data - web scraping
                 [org.clj-commons/hickory "0.7.4"]

                 ;; parsing strings
                 [instaparse "1.5.0"]

                 ]
  :aot [afrolabs.components.kafka.json-serdes
        afrolabs.components.kafka.edn-serdes
        afrolabs.components.kafka.bytes-serdes
        afrolabs.components.confluent.schema-registry-compatible-serdes]
  :repl-options {}
  :profiles {:dev {:dependencies [[com.clojure-goes-fast/clj-java-decompiler "0.3.4"]]}})
