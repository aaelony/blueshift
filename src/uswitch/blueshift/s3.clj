(ns uswitch.blueshift.s3
  (:require [com.stuartsierra.component :refer (Lifecycle system-map using start stop)]
            [clojure.tools.logging :refer (info error warn debug errorf)]
            [aws.sdk.s3 :refer (list-objects get-object delete-object)]
            [clojure.set :refer (difference)]
            [clojure.core.async :refer (go-loop thread put! chan >!! <!! >! <! alts!! timeout close!)]
            [clojure.edn :as edn]
            [uswitch.blueshift.util :refer (close-channels clear-keys)]
            [schema.core :as s]
            [metrics.counters :refer (counter inc! dec!)]
            [metrics.timers :refer (timer time!)]
            [uswitch.blueshift.redshift :as redshift]
            )        
  (:import [java.io PushbackReader InputStreamReader]
           [org.apache.http.conn ConnectionPoolTimeoutException]))

(defrecord Manifest [table pk-columns columns jdbc-url options data-pattern strategy database])


(def ManifestSchema {:table        s/Str
                     :pk-columns   [s/Str]
                     :columns      [s/Str]
                     :database     s/Keyword   ;; Value of :database key will provide for runtime assoc of :jdbc-url
                     :jdbc-url     s/Str
                     :strategy     s/Str
                     :options      s/Any
                     :data-pattern s/Regex})

(defn validate [manifest]
  (when-let [error-info (s/check ManifestSchema manifest)]
    (throw (ex-info "Invalid manifest. Check map for more details." error-info))))

(defn listing
  [credentials bucket & opts]
  (let [options (apply hash-map opts)]
    (loop [marker   nil
           results  nil]
      (let [{:keys [next-marker truncated? objects]}
            (list-objects credentials bucket (assoc options :marker marker))]
        (if (not truncated?)
          (concat results objects)
          (recur next-marker (concat results objects)))))))

(defn files [credentials bucket directory]
  (listing credentials bucket :prefix directory))

(defn directories
  ([credentials bucket]
     (:common-prefixes (list-objects credentials bucket {:delimiter "/"})))
  ([credentials bucket path]
     {:pre [(.endsWith path "/")]}
     (:common-prefixes (list-objects credentials bucket {:delimiter "/" :prefix path}))))

(defn leaf-directories
  [credentials bucket]
  (loop [work (directories credentials bucket)
         result nil]
    (if (seq work)
      (let [sub-dirs (directories credentials bucket (first work))]
        (recur (concat (rest work) sub-dirs)
               (if (seq sub-dirs)
                 result
                 (cons (first work) result))))
      result)))

(defn read-edn [stream]
  (edn/read (PushbackReader. (InputStreamReader. stream))))

(defn assoc-if-nil [record key value]
  (if (nil? (key record))
    (assoc record key value)
    record))

  
(defn get-jdbc-url [manifest redshift-connections-map]
  (first
   (remove #(nil? %)
           (map (fn [m]
                  (let [k (first (keys m))]
                    (if (= k (:database manifest) )
                      (get-in m [k :jdbc-url]))))
                redshift-connections-map))))

  
(defn manifest [credentials bucket files redshift-connections-map]
  (letfn [(manifest? [{:keys [key]}]
            (re-matches #".*manifest\.edn$" key))]
    (when-let [manifest-file-key (:key (first (filter manifest? files)))]
      (with-open [content (:content (get-object credentials bucket manifest-file-key))]        
        (let [mani (-> (read-edn content)
                       (map->Manifest)
                       (assoc-if-nil :strategy "merge")
                       (update-in [:data-pattern] re-pattern))
              manifest-db-kw (:database mani)
              jdbc-url (get-jdbc-url mani redshift-connections-map)
              ]
          (assoc-if-nil mani :jdbc-url jdbc-url) 
          )))))


 


(defn- step-scan
  [credentials bucket directory redshift-connections-map]
  (debug "Calling uswitch.blueshift.redshift/step-scan for bucket " bucket " and directory " directory )
  (try
    (let [fs (files credentials bucket directory)]
      (if-let [manifest (manifest credentials bucket fs redshift-connections-map)]
        (do
          (debug (str "about to validate the manifest: " (pr-str manifest)))
          (validate manifest)
          (debug (str "done validating manifest for bucket " bucket ))
          (let [data-files  (filter (fn [{:keys [key]}]
                                      (re-matches (:data-pattern manifest) key))
                                    fs)]
            (info (str "There were " (count data-files) " matching the :data-pattern in the manifest in dir " directory ))
            (if (seq data-files)
              (do
                (debug "Watcher triggering import" (:table manifest))
                (debug "Triggering load:" load)
                {:state :load, :table-manifest manifest, :files (map :key data-files)})
              {:state :scan, :pause? true})))
        {:state :scan, :pause? true}))
    (catch clojure.lang.ExceptionInfo e
      (error e "Error with manifest file")
      {:state :scan, :pause? true})
    (catch ConnectionPoolTimeoutException e
      (warn e "Connection timed out. Will re-try.")
      {:state :scan, :pause? true})
    (catch Exception e
      (error e "Failed reading content of" (str bucket "/" directory))
      {:state :scan, :pause? true})))

(def importing-files (counter [(str *ns*) "importing-files" "files"]))
(def import-timer (timer [(str *ns*) "importing-files" "time"]))


(defn- step-load
  [credentials bucket table-manifest files server-side-encryption]
  (let [redshift-manifest-contents  (redshift/manifest bucket files)
        redshift-manifest-filename (->> (-> (str bucket "--" (first files))
                                 (clojure.string/split #"/") 
                                 butlast) 
                             (interpose "--") (apply str) )

        {:keys [key url]}  (redshift/put-manifest credentials bucket redshift-manifest-contents server-side-encryption redshift-manifest-filename)]

    (info "Attempting to import " (count files) "data files to table" (:table table-manifest) "from redshift-manifest-contents" url)
    (debug "Importing Redshift Manifest" redshift-manifest-contents)
    (inc! importing-files (count files))
    (try (time! import-timer
                (redshift/load-table credentials url table-manifest))
         (info "Successfully imported" (count files) "files")
         (delete-object credentials bucket key)  ;; delete the redshift manifest.
         (dec! importing-files (count files))
         {:state :delete
          :files files}
         (catch java.sql.SQLException e
           (error e "Error loading into table" (:table table-manifest))
           (error (:table table-manifest) "Redshift manifest content:" redshift-manifest-contents)
           (delete-object credentials bucket key)  ;; deletes the redshift manifest file it had just created.
           (dec! importing-files (count files))
           {:state :scan
            :pause? true}))))

(defn- step-delete
  "Deletes file objects from an S3 bucket. Make sure you want to do this!"
  [credentials bucket files]
  (do
    (doseq [key files]
      (info "Deleting" (str "s3://" bucket "/" key))
      (try
        (delete-object credentials bucket key)
        (catch Exception e
          (warn "Couldn't delete" key "  - ignoring"))))
    {:state :scan, :pause? true}))

(defn- progress
  [{:keys [state] :as world}
   {:keys [credentials bucket directory redshift-connections-map server-side-encryption keep-s3-files-on-import] :as configuration}]
  (debug (str "Progress state is " state))
  (case state
    :scan   (step-scan   credentials bucket directory redshift-connections-map)
    :load   (step-load   credentials bucket           (:table-manifest world) (:files world) server-side-encryption) 
    ;; :delete (step-delete credentials bucket           (:files world))
    :delete (if (not= true keep-s3-files-on-import)
              (do
                (warn (str "Will not delete existing s3 files in bucket" bucket ", because config :keep-files-on-import is set to " (get world :keep-files-on-import)))
                (step-delete credentials bucket           (:files world))
                )
              )
    ))

(defrecord KeyWatcher [credentials bucket directory poll-interval-seconds redshift-connections-map server-side-encryption keep-s3-files-on-import]
  Lifecycle
  (start [this]
    (debug "Starting KeyWatcher for" (str bucket "/" directory) "polling every" poll-interval-seconds "seconds")
    (let [control-ch    (chan)
          configuration {:credentials credentials :bucket bucket :directory directory 
                         :redshift-connections-map redshift-connections-map :server-side-encryption server-side-encryption
                         :keep-s3-files-on-import keep-s3-files-on-import}]
      (thread
       (loop [timer (timeout (* poll-interval-seconds 1000))
              world {:state :scan}]
         (let [next-world (progress world configuration)]
           (if (:pause? next-world)
             (let [[_ c] (alts!! [control-ch timer])]
               (when (not= c control-ch)
                 (recur (timeout (* poll-interval-seconds 1000)) next-world)))
             (recur timer next-world)))))
      (assoc this :watcher-control-ch control-ch)))
  (stop [this]
    (debug "Stopping KeyWatcher for" (str bucket "/" directory))
    (close-channels this :watcher-control-ch)))


(defn spawn-key-watcher! [credentials bucket directory poll-interval-seconds redshift-connections-map server-side-encryption keep-s3-files-on-import]
  (start (KeyWatcher. credentials bucket directory poll-interval-seconds redshift-connections-map server-side-encryption keep-s3-files-on-import)))

(def directories-watched (counter [(str *ns*) "directories-watched" "directories"]))

(defrecord KeyWatcherSpawner [bucket-watcher poll-interval-seconds]
  Lifecycle
  (start [this]
    (debug "Starting KeyWatcherSpawner")
    (let [{:keys [new-directories-ch bucket credentials redshift-connections-map 
                  server-side-encryption keep-s3-files-on-import]} bucket-watcher
          watchers (atom nil)]
      (go-loop [dirs (<! new-directories-ch)]
        (when dirs
          (doseq [dir dirs]
            (swap! watchers conj (spawn-key-watcher! credentials bucket dir poll-interval-seconds 
                                                     redshift-connections-map server-side-encryption keep-s3-files-on-import))
            (inc! directories-watched))
          (recur (<! new-directories-ch))))
      (assoc this :watchers watchers)))
  (stop [this]
    (debug "Stopping KeyWatcherSpawner")
    (when-let [watchers (:watchers this)]
      (info "Stopping" (count @watchers) "watchers")
      (doseq [watcher @watchers]
        (stop watcher)
        (dec! directories-watched)))
    (clear-keys this :watchers)))

(defn key-watcher-spawner [config]
  (map->KeyWatcherSpawner {:poll-interval-seconds (-> config :s3 :poll-interval :seconds)}))

(defn matching-directories [credentials bucket key-pattern]
  (try (->> (leaf-directories credentials bucket)
            (filter #(re-matches key-pattern %))
            (set))
       (catch Exception e
         (errorf e "Error checking for matching object keys in \"%s\"" bucket)
         #{})))

(defrecord BucketWatcher [credentials bucket key-pattern poll-interval-seconds 
                          redshift-connections-map server-side-encryption keep-s3-files-on-import]
  Lifecycle
  (start [this]
    (debug "Starting BucketWatcher. Polling" bucket "every" poll-interval-seconds "seconds for keys matching" key-pattern)
    (let [new-directories-ch (chan)
          control-ch         (chan)]
      (thread
        (loop [dirs nil]
          (let [available-dirs (matching-directories credentials bucket key-pattern)
                new-dirs       (difference available-dirs dirs)]
            (when (seq new-dirs)
              (debug "New directories:" new-dirs "spawning" (count new-dirs) "watchers")
              (>!! new-directories-ch new-dirs))
            (let [[v c] (alts!! [(timeout (* 1000 poll-interval-seconds)) control-ch])]
              (when-not (= c control-ch)
                (recur available-dirs))))))
      (assoc this :control-ch control-ch :new-directories-ch new-directories-ch)))
  (stop [this]
    (debug "Stopping BucketWatcher")
    (close-channels this :control-ch :new-directories-ch)))

(defn bucket-watcher
  "Creates a process watching for objects in S3 buckets."
  [config]
  (map->BucketWatcher {:credentials (-> config :s3 :credentials)
                       :bucket (-> config :s3 :bucket)
                       :redshift-connections-map (-> config :redshift-connections)
                       :poll-interval-seconds (-> config :s3 :poll-interval :seconds)
                       :keep-s3-files-on-import (-> config :s3 :keep-s3-files-on-import)
                       :server-side-encryption (or (-> config :s3 :server-side-encryption) nil)
                       :key-pattern (or (re-pattern (-> config :s3 :key-pattern))
                                        #".*")}))

(defrecord PrintSink [prefix chan-k component]
  Lifecycle
  (start [this]
    (let [ch (get component chan-k)]
      (go-loop [msg (<! ch)]
        (when msg
          (info prefix msg)
          (recur (<! ch)))))
    this)
  (stop [this]
    this))

(defn print-sink
  [prefix chan-k]
  (map->PrintSink {:prefix prefix :chan-k chan-k}))

(defn s3-system [config]
  (system-map :bucket-watcher (bucket-watcher config)
              :key-watcher-spawner (using (key-watcher-spawner config)
                                          [:bucket-watcher])))
;; (bucket-watcher config)
