(defproject afrolabs/components "0.1.0-SNAPSHOT"
  :description "Shared clojure components"
  :url "https://github.com/Afrolabs/afrolabs-clj"
  :license {:name "MIT"
            :url "https://github.com/Afrolabs/afrolabs-clj/blob/main/LICENSE"}
  :repositories [["confluent" "https://packages.confluent.io/maven/"]]
  :dependencies [[org.clojure/clojure "1.11.1"]

                 ;; property-based testing, generative testing
                 [org.clojure/test.check "1.1.1"]

                 ;; dependency conflict resolution, also edn reader
                 [org.clojure/tools.reader "1.3.6"]

                 [org.eclipse.jetty/jetty-util "9.4.44.v20210927"]
                 [org.eclipse.jetty/jetty-http "9.4.44.v20210927"]
                 [org.eclipse.jetty/jetty-client "9.4.44.v20210927"]

                 ;; conversions between different kinds of casing
                 ;; camelCase -> snake_case -> kebab-case -> PascalCase
                 [camel-snake-kebab "0.4.2"]


                 [metosin/jsonista "0.3.5" :exclusions [com.fasterxml.jackson.core/jackson-core]]

                 ;; monadic utilities for validation, failure and error checking
                 ;; https://github.com/adambard/failjure
                 [failjure "2.2.0"]

                 ;; facilities for asynchronous programming
                 ;; aka golang channels for clojure
                 ;; https://clojure.org/news/2013/06/28/clojure-clore-async-channels
                 [org.clojure/core.async "1.5.648"]

                 ;; https://github.com/cgrand/xforms ; sometimes useful extra transducers - data transformation
                 [net.cgrand/xforms "0.19.2"]

                 ;; https://github.com/redplanetlabs/specter, data transformation DSL
                 [com.rpl/specter "1.1.4"]

                 ;; https://github.com/weavejester/integrant
                 ;; similar to dependency injection
                 [integrant "0.8.0"]

                 ;; aero - a lib for loading configuration (eg integrant config) from edn files
                 [aero "1.1.6"]

                 ;; handling of UNIX process signals like SIGINT
                 [beckon "0.1.1"]

                 ;; https://github.com/http-kit/http-kit
                 ;; http server
                 [http-kit "2.5.3"]

                 ;; password-hashing helpers
                 [buddy/buddy-hashers "1.8.158"]
                 [buddy/buddy-auth "3.0.323"]
                 [buddy/buddy-sign "3.4.333"]

                 [ring/ring-core "1.9.5"]

                 ;; https://github.com/metosin/ring-http-response
                 ;; ring response helper fns
                 [metosin/ring-http-response "0.9.3"]

                 ;; http routing library
                 ;; https://github.com/metosin/reitit
                 [metosin/reitit "0.5.18" :exclusions [com.fasterxml.jackson.core/jackson-core]]

                 ;; https://github.com/dm3/clojure.java-time
                 [clojure.java-time "0.3.3"]
                 [org.threeten/threeten-extra "1.7.0"]

                 ;; https://github.com/clj-commons/iapetos
                 [io.prometheus/simpleclient "0.15.0"]
                 [io.prometheus/simpleclient_hotspot "0.15.0"]
                 [clj-commons/iapetos "0.1.12"]

                 ;; cognitect (custodians of clojure) has a very nice data driven API interface to AWS
                 [com.cognitect.aws/api "0.8.539"]
                 [com.cognitect.aws/endpoints "1.1.12.206"]
                 [com.cognitect.aws/sns "822.2.1109.0"]
                 [com.cognitect.aws/sqs "822.2.1109.0"]
                 [com.cognitect.aws/s3 "822.2.1109.0"]

                 ;; NREPL
                 [nrepl "0.9.0"]

                 ;; logging for clj(s) https://github.com/ptaoussanis/timbre
                 [com.taoensso/timbre "5.2.1"]

                 ;; https://github.com/clojure/data.json
                 [org.clojure/data.json "2.4.0"]

                 ;; https://github.com/clojure/data.csv
                 [org.clojure/data.csv "1.0.1"]

                 ;; malli; schema-driven development and utilities
                 [metosin/malli "0.8.4"]

                 ;; hooking in the result of other java logging frameworks into slf4j
                 [org.slf4j/log4j-over-slf4j "1.7.36"]
                 [org.slf4j/jul-to-slf4j "1.7.36"]
                 [org.slf4j/jcl-over-slf4j "1.7.36"]

                 ;; then routing slf4j to timbre
                 ;; This will allow us to receive, log (and configure) the logs provided by libraries
                 ;; as well as the app logs we create.
                 [com.fzakaria/slf4j-timbre "0.3.21"]

                 ;; possibility to log to json, for use in cloud environments like GCK
                 [viesti/timbre-json-appender "0.2.6"]

                 [com.fasterxml.jackson.core/jackson-databind "2.13.2.2"]
                 [com.fasterxml.jackson.datatype/jackson-datatype-jsr310 "2.13.2"]

                 ;; https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
                 [org.apache.kafka/kafka-clients "3.1.0"]


                 ;; following dependencies loosely inspired by the confluent example java app
                 ;; https://github.com/confluentinc/examples/blob/6.1.1-post/clients/cloud/java/pom.xml
                 [org.apache.kafka/kafka-streams "3.1.0" ]

                 ;; dependency issue resolutions brought on by confluent
                 [org.javassist/javassist "3.26.0-GA"]
                 [jakarta.xml.bind/jakarta.xml.bind-api "2.3.3" :upgrade false]
                 [joda-time "2.10.14"]

                 ;; dependencies for confluent cloud
                 [org.apache.kafka/connect-runtime "3.1.0" :exclusions [org.slf4j/slf4j-log4j12]]
                 [io.confluent/kafka-json-serializer "7.0.1"  :exclusions []]
                 [io.confluent/kafka-json-schema-serializer "7.0.1" :exclusions [org.apache.kafka/kafka-clients
                                                                                 org.glassfish.jersey.core/jersey-common]]

                 ;; for use with ksqld, the java client
                 [io.confluent.ksql/ksqldb-api-client "7.0.1" :exclusions [org.slf4j/slf4j-log4j12]]

                 [org.slf4j/slf4j-api "1.7.36"]
                 ;; [org.slf4j/slf4j-log4j12 "1.7.32"]
                 [io.confluent/confluent-log4j "1.2.17-cp2"]

                 
                 [thheller/shadow-cljs "2.18.0"]

                 ;; middleware that adds "X-Clacks-Overhead" to responses
                 [gsnewmark/ring-pratchett "0.1.0"]

                 ]
  :aot [afrolabs.components.kafka.json-serdes
        afrolabs.components.kafka.edn-serdes
        afrolabs.components.kafka.bytes-serdes
        afrolabs.components.confluent.schema-registry-compatible-serdes]
  :repl-options {}
  :profiles {:dev {:dependencies [[com.clojure-goes-fast/clj-java-decompiler "0.3.1"]]}})
