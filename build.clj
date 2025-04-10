(ns build
  (:require
   [clojure.tools.build.api :as build-api]
   [clojure.pprint :as pprint]))

(def project-config
  "Project configuration to support build tasks."
  {:class-directory "target/classes"
   :lib             com.github.gometro/afrolabs-clj
   :url             "https://github.com/Afrolabs/afrolabs-clj"
   :description     "A library of components useful for making micro-services."
   :licenses        [{:name "MIT License"
                      :url  "https://github.com/Afrolabs/afrolabs-clj/blob/main/LICENSE"}]
   :project-basis   (build-api/create-basis {:project "deps.edn"})})

(defn config
  "Display build configuration"
  [config]
  (pprint/pprint (or config project-config)))

(defn clean
  "Remove a directory
  - `:path '\"directory-name\"'` for a specific directory
  - `nil` (or no command line arguments) to delete `target` directory
  `target` is the default directory for build artefacts
  Checks that `.` and `/` directories are not deleted"
  [directory]
  (when-not (contains? #{"." "/"} directory)
    (build-api/delete {:path (or (:path directory)
                                 "target")})))

(defn compile-aot
  "Pre-compile namespace that generate .class files."
  [& _]
  (let [{:keys [project-basis
                class-directory]} project-config]
    (build-api/compile-clj {:basis      project-basis
                            :class-dir  class-directory
                            :ns-compile '[afrolabs.components.kafka.json-serdes
                                          afrolabs.components.kafka.edn-serdes
                                          afrolabs.components.kafka.bytes-serdes
                                          afrolabs.components.kafka.transit-serdes
                                          #_afrolabs.components.confluent.schema-registry-compatible-serdes]})))
