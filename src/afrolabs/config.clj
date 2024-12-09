(ns afrolabs.config
  (:require
   [aero.core :as aero]
   [clojure.data.csv :as csv]
   [clojure.edn :as edn]
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [integrant.core :as ig]
   [org.httpkit.client :as http]
   [taoensso.timbre :as log]
   ))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Copied from the source code for weavejester/environ
;; https://github.com/weavejester/environ

(defn- keywordize
  [s]
  (-> (str/lower-case s)
      (str/replace "_" "-")
      (str/replace "." "-")
      (keyword)))

(defn- read-system-env
  []
  (->> (System/getenv)
       (map (fn [[k v]] [(keywordize k) v]))
       (into {})))

(defn- read-system-props
  []
  (->> (System/getProperties)
       (map (fn [[k v]] [(keywordize k) v]))
       (into {})))

(defn- read-dotenv-file
  [dotenv-file-name]
  (let [f (clojure.java.io/as-file dotenv-file-name)]
    (if-not (.exists f) {}
            (with-open [f (clojure.java.io/reader f)]
              (->> (line-seq f)

                   (map str/trim)

                   ;; remove empty lines
                   (filter #(pos? (count %)))

                   ;; filter comment lines
                   (filter (complement #(.startsWith % "#")))

                   ;; split on the first '='
                   (map (fn [s]
                          (let [i (str/index-of s "=")]
                            [(keywordize (subs s 0 i))
                             (subs s (inc i))])))
                   (into {}))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmethod aero/reader 'parameter
  [{ps :parameters} _ value]
  (when-not ps
    (throw (ex-info "Give the :parameters option value to the aero read-config file." {})))
  (let [result (get ps (keywordize (str value)))]
    (when-not result
      (throw (ex-info (format "The parameter '%s' can not be resolved to a value." (str value))
                      {:all-keys (keys ps)})))
    result))

(defmethod aero/reader 'option
  [{ps :parameters} _ value]
  (when-not ps
    (throw (ex-info "Give the :parameters option value to the aero read-config file." {})))
  (get ps (keywordize (str value))))

(defmethod aero/reader 'ig/ref
  [_ _ value]
  (ig/ref value))

(defmethod aero/reader 'regex
  [_ _ value] (re-pattern value))

(defn interpret-string-as-csv-row
  [row-string]
  (first (csv/read-csv (java.io.StringReader. row-string))))

(defmethod aero/reader 'csv-array
  [_ _ value]
  (interpret-string-as-csv-row value))

(defmethod aero/reader 'long?
  [_ _ value]
  (when (and value
             (seq value))
    (Long/parseLong (str value))))

(defmethod aero/reader 'edn
  [_ _ value]
  (when value
    (edn/read-string value)))

(defmethod aero/reader 'ip-hostname
  [_ _ _value]
  (.getHostName (java.net.InetAddress/getLocalHost)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Queries the EC2 instance metadata. This has a fixed IP-based URL. Returns \"not-ec2-instance\" if this call does not work.
(def instance-id-values
  (delay
    (let [{:keys [status error body]}
          @(http/get "http://169.254.169.254/latest/dynamic/instance-identity/document")]
      (if (or error (not= 200 status))
        nil
        (json/read-str body)))))

(defmethod aero/reader 'ec2/instance-identity-data
  [_ _ value] (get @instance-id-values value))

;; #?(:clj (Long/parseLong (str value)))
;; #?(:cljs (js/parseInt (str value)))

(defn config-string->bool
  [value]
  (boolean (cond
             (nil? value)     nil ;; (boolean nil) -> false
             (boolean? value) value
             ;; environment variables come as strings.
             (string? value)  (#{"ON" "1" "TRUE"}     ;; any envvar value not exactly these ones will be false
                               (str/upper-case value))
             (pos-int? value) true
             (zero?    value) false
             :else            value)))

(defmethod aero/reader 'bool
  [_ _ value]
  (config-string->bool value))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmethod aero/reader 'config/pick
  [_opts _ value]
  (when-not (odd? (count value))
    (throw (ex-info "Provide the case value and the cases as pairs." {})))
  (let [[the-case & cases] value
        all-cases (into {} (map vec (partition 2 cases)))]
    (or (get all-cases the-case)
        (throw (ex-info "No case found"
                        {:the-case  the-case
                         :cases     cases
                         :all-cases all-cases})))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defonce static-parameter-sources (delay (merge (read-system-env)
                                                (read-system-props))))

(defn read-parameter-sources
  [dotenv-file]
  (merge @static-parameter-sources
         (if-not dotenv-file {}
                 (read-dotenv-file dotenv-file))))

(defn read-config
  [config-file-location
   & {:keys [profile
             dotenv-file
             cli-args]
      :or {profile :dev
           dotenv-file ".env"
           cli-args {}}}]
  (when-not (and config-file-location
                 (clojure.java.io/resource config-file-location))
    (throw (ex-info "Specify a valid config-file-location"
                    (cond-> {}
                      (not config-file-location)
                      (assoc ::reason "Value not specified : nil")

                      (not (clojure.java.io/resource config-file-location))
                      (assoc ::reason (str "No resource found at : " config-file-location))))))

  (let [parameters (merge (read-parameter-sources dotenv-file)
                          cli-args)]
    (aero/read-config (try
                        (clojure.java.io/resource config-file-location)
                        (catch Throwable e

                          (log/error e "Cannot open the config resource file.")
                          ))
                      {:profile    profile
                       :parameters parameters})))

(defn with-config
  "Reads a dotenv-file and provides it to a callback. Useful in repl development testing scenarios."
  ([dotenv-file callback]
   (callback (read-parameter-sources dotenv-file)))
  ([callback] (with-config ".env" callback)))

(defn load-config
  "Reads a dotenv-file and returns it with the static parameter context."
  ([] (load-config ".env"))
  ([dotenv-file]
   (read-parameter-sources dotenv-file)))
