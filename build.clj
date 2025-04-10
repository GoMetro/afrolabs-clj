(ns build
  (:require
   [clojure.tools.build.api :as build-api]))

(def lib   'com.github.gometro/afrolabs-clj)
(def version (format "1.0.%s" (build-api/git-count-revs nil)))
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar"
                      (name lib)
                      version))
(def basis (delay (build-api/create-basis {:project "deps.edn"})))


(def project-config
  "Project configuration to support build tasks."
  {:class-directory "target/classes"
   :url             "https://github.com/Afrolabs/afrolabs-clj"
   :description     "A library of components useful for making micro-services."
   :licenses        [{:name "MIT License"
                      :url  "https://github.com/Afrolabs/afrolabs-clj/blob/main/LICENSE"}]
   :project-basis   (build-api/create-basis {:project "deps.edn"})})

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

(defn pom
  [{:as   _opts
    :keys [dest]}]

  (build-api/write-pom (cond-> {:lib       lib
                                :version   version
                                :basis     @basis
                                :src-dirs  ["src"]}
                         dest (assoc :target dest)
                         (not dest) (assoc :class-dir class-dir))))

(defn compile-aot
  "Pre-compile namespace that generate .class files."
  [& _]
  (build-api/compile-clj {:basis      @basis
                          :class-dir   class-dir
                          :ns-compile '[afrolabs.components.kafka.json-serdes
                                        afrolabs.components.kafka.edn-serdes
                                        afrolabs.components.kafka.bytes-serdes
                                        afrolabs.components.kafka.transit-serdes
                                        #_afrolabs.components.confluent.schema-registry-compatible-serdes]}))
