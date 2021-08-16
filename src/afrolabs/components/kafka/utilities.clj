(ns afrolabs.components.kafka.utilities
  (:require [afrolabs.components.kafka :as k]
            [integrant.core :as ig]
            [clojure.data.json :as json]
            [taoensso.timbre :as timbre
             :refer [log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]))

(defn load-messages-from-confluent-topic
  [& {:keys [bootstrap-server
             topics
             nr-msgs
             api-key api-secret
             extra-strategies]
      :or {nr-msgs          10
           extra-strategies []}}]
  (let [loaded-msgs (atom nil)
        loaded-enough-msgs (promise)

        consumer-client
        (reify
          k/IConsumerClient
          (consume-messages
              [_ msgs]
            (let [new-state (swap! loaded-msgs (partial apply conj) msgs)
                  how-many (count new-state)]
              (when (< nr-msgs how-many)
                (info "Indicating that we've received enough messages...")
                (deliver loaded-enough-msgs true)))
            nil))

        ig-cfg
        {::k/kafka-consumer
         {:bootstrap-server               bootstrap-server
          :consumer/client                consumer-client
          :strategies                     (concat [(k/ConfluentCloud :api-key api-key :api-secret api-secret)
                                                   (k/SubscribeWithTopicsCollection topics)
                                                   (k/FreshConsumerGroup)
                                                   (k/OffsetReset "earliest")]
                                                  extra-strategies)}}

        system (ig/init ig-cfg)]

    (try
      @loaded-enough-msgs
      (info "Done waiting, received enough messages.")
      (ig/halt! system)
      (info "System done shutting down.")

      ;; return value
      @loaded-msgs

      (catch Throwable t
        (warn t "Caught a throwable while waiting for messages to load. Stopping the system...")
        (ig/halt! system)
        (info "Extraordinary system stop completed.")))))

(comment

  (defonce username-password (atom nil))
  (defn set-creds [un pw] (reset! username-password [un pw]))

  (def ctrack-msgs (load-messages-from-topic :topics ["dataprovider_ctrack"]
                                             :bootstrap-server "pkc-l6wr6.europe-west2.gcp.confluent.cloud:9092"
                                             :api-key (first @username-password)
                                             :api-secret (second @username-password)
                                             :nr-msgs 1000))
  (count ctrack-msgs)

  (def netstar-msgs (load-messages-from-topic :topics ["dataprovider_netstar"]
                                              :bootstrap-server "pkc-l6wr6.europe-west2.gcp.confluent.cloud:9092"
                                              :api-key (first @username-password)
                                              :api-secret (second @username-password)
                                              :nr-msgs 1000))
  (count netstar-msgs)

  (first netstar-msgs)

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  (defn sample [msgs & {:keys [how-many]}]
    (into []
          (comp
           (if how-many (take how-many) identity)
           (map #(update % :value json/read-str))
           (map #(update-in % [:value "MessageData"] json/read-str))
           (map :value))
          msgs))

  (require '[malli.provider :as mp])

  (def ctrack-sample (sample ctrack-msgs :how-many 20))
  (def netstar-sample (sample netstar-msgs :how-many 20))

  (first ctrack-sample)
  (second netstar-sample)

  (mp/provide ctrack-sample)
  (mp/provide netstar-sample)


  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  (require '[malli.json-schema :as mjson])

  ;; for future reference
  (def ctrack-schema [:map
                      ["MessageData"
                       [:map
                        ["DriverId" {:optional true} string?]
                        ["Ignition" boolean?]
                        ["Longitude" double?]
                        ["LocationString" string?]
                        ["NodeId" int?]
                        ["Speed" {:optional true} int?]
                        ["StatusText" string?]
                        ["DeliveryTag" int?]
                        ["EventTimeUTC" string?]
                        ["VehicleHeading" string?]
                        ["Odo" int?]
                        ["SingleStatus" int?]
                        ["Latitude" double?]
                        ["StreetMaxSpeed" {:optional true} int?]]]
                      ["AttributesMap" [:map]]])

  (mjson/transform ctrack-schema)

  (def netstar-schema [:map
                       ["MessageData"
                        [:map
                         ["DriverId" string?]
                         ["$type" string?]
                         ["Address" nil?]
                         ["Battery" double?]
                         ["Temperatures" [:map ["$type" string?] ["$values" [:vector any?]]]]
                         ["IsIgnitionOn" boolean?]
                         ["Imei" int?]
                         ["Longitude" double?]
                         ["HardwareAttachedGpsNotPresent" string?]
                         ["AccidentId" int?]
                         ["Altitude" double?]
                         ["GpsLock" nil?]
                         ["EngineHours" double?]
                         ["Speed" double?]
                         ["HardwareAttached" string?]
                         ["TotalOdometer" double?]
                         ["Odometer" double?]
                         ["NoofSatellites" nil?]
                         ["UnitType" int?]
                         ["Samples" [:map ["$type" string?] ["$values" [:vector any?]]]]
                         ["AmbientTemperature" int?]
                         ["Forces"
                          [:map
                           ["$type" string?]
                           ["$values"
                            [:vector
                             [:map
                              ["$type" string?]
                              ["Forward" int?]
                              ["Backward" int?]
                              ["Left" int?]
                              ["Right" int?]
                              ["Up" int?]
                              ["Down" int?]]]]]]
                         ["Hdop" nil?]
                         ["InputOutputs"
                          [:map
                           ["$type" string?]
                           ["DigitalOutput3" boolean?]
                           ["DigitalOutput4" boolean?]
                           ["DigitalInput1" boolean?]
                           ["AnalogueInput2" boolean?]
                           ["DigitalInput3" boolean?]
                           ["DigitalOutput1" boolean?]
                           ["AnalogueInput3" boolean?]
                           ["AnalogueInput4" boolean?]
                           ["DigitalInput4" boolean?]
                           ["AnalogueInput1" boolean?]
                           ["DigitalInput2" boolean?]
                           ["DigitalOutput2" boolean?]]]
                         ["CanBus" nil?]
                         ["ServerDateTime" string?]
                         ["Latitude" double?]
                         ["IsLastKnowLocation" boolean?]
                         ["VbuNo" int?]
                         ["HardwareAttachedUptime" string?]
                         ["EngineTemperature" double?]
                         ["EventId" int?]
                         ["FuelUsed" int?]
                         ["Id" string?]
                         ["Heading" double?]
                         ["HardwareAttachedNoGsmInternationalRoaming" string?]
                         ["DateTime" string?]]]
                       ["AttributesMap" [:map]]])

  (mjson/transform netstar-schema)



  )
