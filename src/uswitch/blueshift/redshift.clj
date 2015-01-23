(ns uswitch.blueshift.redshift
  (:require [aws.sdk.s3 :refer (put-object delete-object)]
            [cheshire.core :refer (generate-string)]
            [clojure.tools.logging :refer (info error debug)]
            [clojure.string :as s]
            [com.stuartsierra.component :refer (system-map Lifecycle using)]
            [clojure.core.async :refer (chan <!! >!! close! thread)]
            [uswitch.blueshift.util :refer (close-channels)]
            [metrics.meters :refer (mark! meter)]
            [metrics.counters :refer (inc! dec! counter)]
            [metrics.timers :refer (timer time!)])
  (:import [java.util UUID]
           [java.sql DriverManager SQLException]))


(defn manifest [bucket files]
  (debug (str "Calling uswitch.blueshift.redshift/manifest " bucket " for files: " (prn-str files )))
  {:entries (for [f files] {:url (str "s3://" bucket "/" f)
                            :mandatory true})})

(defn put-manifest
  "Uploads the Amazon Redshift manifest to S3 as JSON, returns the URL to the uploaded object.
   Manifest should be generated with uswitch.blueshift.redshift/manifest."
  [credentials bucket manifest server-side-encryption redshift-manifest-filename]
  (debug (str "Calling uswitch.blueshift.redshift/put-manifest for bucket " bucket " manifest: " manifest ))
  (let [;;file-name (str (UUID/randomUUID) ".manifest")
        file-name (str redshift-manifest-filename "-" (UUID/randomUUID) ".manifest")
        s3-url (str "s3://" bucket "/" file-name)
        options-map (if (not (nil? server-side-encryption))
                      {:server-side-encryption server-side-encryption} ) ]
    (info (str "Redshift-manifest for " bucket " is " file-name ))
    ;;(put-object credentials bucket file-name (generate-string manifest) {:server-side-encryption "AES256"} )
    (put-object credentials bucket file-name (generate-string manifest) options-map )
    {:key file-name
     :url s3-url}))

(def redshift-imports (meter [(str *ns*) "redshift-imports" "imports"]))
(def redshift-import-rollbacks (meter [(str *ns*) "redshift-imports" "rollbacks"]))
(def redshift-import-commits (meter [(str *ns*) "redshift-imports" "commits"]))

;; pgsql driver isn't loaded automatically from classpath
(Class/forName "org.postgresql.Driver")

(defn connection [jdbc-url]
  (debug "Calling uswitch.blueshift.redshift/connection ")
  (doto (DriverManager/getConnection jdbc-url)
    (.setAutoCommit true))) ;; Setting setAutoCommit to true improves performance by mitigating savepoint rollback.

(def ^{:dynamic true} *current-connection* nil)


(defn make-staging-table-name [orig-table-name]
  (let [MAX-REDSHIFT-TABLENAME-LENGTH 127
        max-staging-tablename-length 100
        prefix (str orig-table-name "_staging_" (clojure.string/replace (java.util.UUID/randomUUID) #"-" "_" ))
        x (dec (- max-staging-tablename-length (count prefix)))
        chars-avail (if (> x 0) x 1)
        
        staging-table-name (cond 
                            (< 0 (count prefix) max-staging-tablename-length) prefix
                            :else (subs prefix 0 (- max-staging-tablename-length chars-avail))
                             )
        ]
    (info (str "Staging Table Name: " staging-table-name))
    staging-table-name
        ))
;; (make-staging-table-name "this_is_a_very_very_long_table_name_here_that_is_really_really_quite_longish_longish_and_longer_than_reasonable_unreasonably_so")


(defn prepare-statement
  [sql]
  (.prepareStatement *current-connection* sql))

(defmacro with-connection [jdbc-url & body]
  `(binding [*current-connection* (connection ~jdbc-url)]
     (try ~@body
          (debug "COMMIT")
          (.commit *current-connection*)
          (mark! redshift-import-commits)
          (catch SQLException e#
            (error e# "ROLLBACK")
            (mark! redshift-import-rollbacks)
            (.rollback *current-connection*)
            (throw e#))
          (finally
            (when-not (.isClosed *current-connection*)
              (.close *current-connection*))))))


(defn create-staging-table-stmt [target-table staging-table]
  (debug (str "Calling create-staging-table-stmt for " staging-table " (" target-table ")"))
  (prepare-statement (format "CREATE TEMPORARY TABLE %s (LIKE %s INCLUDING DEFAULTS)"
                             staging-table
                             target-table)))

(defn copy-from-s3-stmt [table manifest-url {:keys [access-key secret-key] :as creds} {:keys [columns options] :as table-manifest}]
  (debug (str "calling uswitch.blueshift.redshift/copy-from-s3-stmt for table " table )) 
  (prepare-statement (format "COPY %s (%s) FROM '%s' CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' %s manifest"
                             table
                             (s/join "," columns)
                             manifest-url
                             access-key
                             secret-key
                             (s/join " " options))))

(defn truncate-table-stmt [target-table]
  (prepare-statement (format "truncate table %s" target-table)))

(defn delete-target-stmt
  "Deletes rows, with the same primary key value(s), from target-table that will be
   overwritten by values in staging-table."
  [target-table staging-table keys]
  (let [where (s/join " AND " (for [pk keys]
                                (str target-table "." pk "=" staging-table "." pk)))]
    (prepare-statement (format "DELETE FROM %s USING %s WHERE %s" target-table staging-table where))))

(defn insert-from-staging-stmt [target-table staging-table]
  (prepare-statement (format "INSERT INTO %s SELECT * FROM %s" target-table staging-table)))

(defn append-from-staging-stmt [target-table staging-table keys]
  (let [join-columns (s/join " AND " (map #(str "s." % " = t." %) keys))
        where-clauses (s/join " AND " (map #(str "t." % " IS NULL") keys))]
    (prepare-statement (format "INSERT INTO %s SELECT s.* FROM %s s LEFT JOIN %s t ON %s WHERE %s"
      target-table staging-table target-table join-columns where-clauses))))

(defn drop-table-stmt [table]
  (prepare-statement (format "DROP TABLE %s" table)))


(defn- jdbc-url-censor
  [s]
  (-> s
      (clojure.string/replace #"user=[^&]*"
                              "user=***")
      (clojure.string/replace #"password=[^&]*"
                              "password=***")))


(defn- aws-censor
  [s]
  (->
   s
   (clojure.string/replace #"aws_access_key_id=[^;]*"
                           "aws_access_key_id=***")
   (clojure.string/replace #"aws_secret_access_key=[^;]*"
                           "aws_secret_access_key=***")))

(defn execute [& statements]
  (doseq [statement statements]
    (debug (aws-censor (.toString statement)))
    (try (.execute statement)
         (catch SQLException e
           (error "Error executing statement:" (.toString statement))
           (throw e)))))

(defn merge-table [credentials redshift-manifest-url {:keys [table jdbc-url pk-columns strategy] :as table-manifest}]
  (let [staging-table (str table "_staging")
        ;; TODO: staging-table (make-staging-table-name table)
        ]
    (debug (str "Calling uswitch.blueshift.redshift/merge-table for " table " via " (jdbc-url-censor jdbc-url) ))
    (mark! redshift-imports)
    (with-connection jdbc-url
      (execute (prepare-statement (format "BEGIN TRANSACTION"))
               (create-staging-table-stmt table staging-table)
               (copy-from-s3-stmt staging-table redshift-manifest-url credentials table-manifest)
               (delete-target-stmt table staging-table pk-columns)
               (insert-from-staging-stmt table staging-table)
               (drop-table-stmt staging-table)
               (prepare-statement (format "END TRANSACTION"))
      ))))

(defn replace-table [credentials redshift-manifest-url {:keys [table jdbc-url pk-columns strategy] :as table-manifest}]
  (mark! redshift-imports)
  (with-connection jdbc-url
    (execute (prepare-statement (format "BEGIN TRANSACTION"))
             (truncate-table-stmt table)
             (copy-from-s3-stmt table redshift-manifest-url credentials table-manifest)
             (prepare-statement (format "END TRANSACTION"))
             )))

(defn append-table [credentials redshift-manifest-url {:keys [table jdbc-url pk-columns strategy] :as table-manifest}]
  (let [staging-table (str table "_staging")
        ;; TODO: staging-table (make-staging-table-name table)
        ]
    (mark! redshift-imports)
    (with-connection jdbc-url
      (execute (prepare-statement (format "BEGIN TRANSACTION"))
               (create-staging-table-stmt table staging-table)
               (copy-from-s3-stmt staging-table redshift-manifest-url credentials table-manifest)
               (append-from-staging-stmt table staging-table pk-columns)
               (drop-table-stmt staging-table)
               (prepare-statement (format "END TRANSACTION"))
      ))))

(defn load-table [credentials redshift-manifest-url {strategy :strategy :as table-manifest}]
  (debug (str "Calling uswitch.blueshift.redshift/load-table with " (keyword strategy) " using " redshift-manifest-url))
  (case (keyword strategy)
    :merge (merge-table credentials redshift-manifest-url table-manifest)
    :replace (replace-table credentials redshift-manifest-url table-manifest)
    :append (append-table credentials redshift-manifest-url table-manifest)))
