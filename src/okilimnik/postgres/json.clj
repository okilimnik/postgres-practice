(ns okilimnik.postgres.json
  (:require
   [jsonista.core :as j]
   [next.jdbc.prepare :as prepare]
   [next.jdbc.result-set :as rs])
  (:import (org.postgresql.util PGobject)
           [java.sql PreparedStatement]))

(set! *warn-on-reflection* true)

(def mapper (j/object-mapper {:decode-key-fn keyword}))
(def ->json j/write-value-as-string)
(def <-json #(j/read-value % mapper))

(defn ->pgobject
  "Transforms Clojure data to a PGobject that contains the data as
  JSON. PGObject type defaults to `jsonb` but can be changed via
  metadata key `:pgtype`"
  [x]
  (let [pgtype (or (:pgtype (meta x)) "jsonb")]
    (doto (PGobject.)
      (.setType pgtype)
      (.setValue (->json x)))))

(defn <-pgobject
  "Transform PGobject containing `json` or `jsonb` value to Clojure data."
  [^PGobject v]
  (let [type  (.getType v)
        value (.getValue v)]
    (if (#{"jsonb" "json"} type)
      (some-> value <-json (with-meta {:pgtype type}))
      value)))

;; if a SQL parameter is a Clojure hash map or vector, it'll be transformed
;; to a PGobject for JSON/JSONB:
(extend-protocol prepare/SettableParameter
  clojure.lang.IPersistentMap
  (set-parameter [m ^PreparedStatement s i]
    (.setObject s i (->pgobject m)))

  clojure.lang.IPersistentVector
  (set-parameter [v ^PreparedStatement s i]
    (.setObject s i (->pgobject v))))

;; if a row contains a PGobject then we'll convert them to Clojure data
;; while reading (if column is either "json" or "jsonb" type):
(extend-protocol rs/ReadableColumn
  org.postgresql.util.PGobject
  (read-column-by-label [^org.postgresql.util.PGobject v _]
    (<-pgobject v))
  (read-column-by-index [^org.postgresql.util.PGobject v _2 _3]
    (<-pgobject v)))

(defn jsonb
  "Accepts jsonb object as Clojure map."
  [m]
  [;; using :lift to tell HoneySQL that this is a value
   :lift
   ;; using metadata to tell JDBC that this is JSONB
   (with-meta m {:pgtype "jsonb"})])

(defn json
  "Accepts json object as Clojure map."
  [m]
  [;; using :lift to tell HoneySQL that this is a value
   :lift
   ;; using metadata to tell JDBC that this is JSON
   (with-meta m {:pgtype "json"})])