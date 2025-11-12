(ns afrolabs.components.confluent.protocols
  "A namespace for some of the clojure protocols to prevent accidentally redefining them at runtime.")

(defprotocol ISubjectJSONSchemaProvider
  "A client protocol to provide a map between SUBJECT names and json schemas. The confluent schema asserter will consume this protocol against the schema registry.

  The result must conform to ::get-subject-json-schemas, which conforms on a collection of maps, each contains a :subject & :schema.

  - Schemas are JSON-encoded strings

  Eg1: [{:subject \"topic-key\"    :schema \"<JSON Schema 1>\"}]
  Eg2: [{:subject \"topic-value\"  :schema \"<JSON Schema 2>\"}]
  Eg3: [{:subject \"some-subject\" :schema \"<JSON Schema 3>\"}
        {:subject \"moar-subject\" :schema \"<JSON Schema 4>\"}] "
  (get-subject-json-schemas [_] "Returns a collection of subject names with JSON schemas in string format."))

(defprotocol IConfluentSchemaAsserter
  "A protocol for using the schema registry, eg mapping from subject names to schema id's"
  (get-schema-id [_ subject-name] "Returns the most recent known schema id for this SUBJECT."))
