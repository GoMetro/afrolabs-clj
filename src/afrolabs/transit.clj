(ns afrolabs.transit
  (:require [cognitect.transit :as t])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream]
           [java.time
            Period
            LocalDate
            LocalDateTime
            ZonedDateTime
            OffsetTime
            Instant
            OffsetDateTime
            ZoneId
            DayOfWeek
            LocalTime
            Month
            Duration
            Year
            YearMonth]))

(def ^:private time-classes
  {'period           {:class    Period
                      :read-fn  #(Period/parse %)}
   'date             {:class   LocalDate
                      :read-fn #(LocalDate/parse %)}
   'date-time        {:class   LocalDateTime
                      :read-fn #(LocalDateTime/parse %)}
   'zoned-date-time  {:class   ZonedDateTime
                      :read-fn #(ZonedDateTime/parse %)}
   'offset-time      {:class   OffsetTime
                      :read-fn #(OffsetTime/parse %)}
   'instant          {:class   Instant
                      :read-fn #(Instant/parse %)}
   'offset-date-time {:class   OffsetDateTime
                      :read-fn #(OffsetDateTime/parse %)}
   'time             {:class   LocalTime
                      :read-fn #(LocalTime/parse %)}
   'duration         {:class   Duration
                      :read-fn #(Duration/parse %)}
   'year             {:class   Year
                      :read-fn #(Year/parse %)}
   'year-month       {:class   YearMonth
                      :read-fn #(YearMonth/parse %)}
   'zone             {:class   ZoneId
                      :read-fn #(ZoneId/of %)}
   'day-of-week      {:class   DayOfWeek
                      :write-fn #(.getValue ^DayOfWeek %)
                      :read-fn #(DayOfWeek/of (int %))}
   'month            {:class   Month
                      :write-fn #(.getValue ^Month %)
                      :read-fn #(Month/of (int %))}})

(def ^:private write-handlers
  (into {}
        (for [[time-name {time-class :class
                          :keys      [write-fn]
                          :or        {write-fn str}}]        time-classes]
          [time-class (t/write-handler (str "time/" time-name) write-fn)])))

(def ^:private read-handlers
  (into {}
        (for [[time-name {:keys [read-fn]}] time-classes]
          [(str "time/" time-name) (t/read-handler read-fn)])))

(defn write-transit [coll output-stream]
  (t/write (t/writer output-stream
                     :json
                     {:handlers write-handlers})
           coll))

(defn write-transit-bytes ^bytes [o]
  (let [os (ByteArrayOutputStream.)]
    (write-transit o os)
    (.toByteArray os)))

(defn write-transit-json ^String [o]
  (String. (write-transit-bytes o) "UTF-8"))

(defn read-transit [input-stream]
  (t/read (t/reader input-stream
                    :json
                    {:handlers read-handlers})))

(defn read-transit-json [^String s]
  (read-transit (ByteArrayInputStream. (.getBytes s "UTF-8"))))

(comment

  (let [data [(Period/ofDays 8)
              (LocalDate/now)
              (LocalDateTime/now)
              (ZonedDateTime/now)
              (Instant/now)
              (OffsetDateTime/now)
              (LocalTime/now)
              (Duration/ofHours 6)
              (Year/now)
              (YearMonth/now)
              (ZoneId/systemDefault)
              (DayOfWeek/SUNDAY)
              (Month/MAY)]
        written (write-transit-bytes data)
        read (read-transit (ByteArrayInputStream. written))]
    (assert (= data read)))

  )
