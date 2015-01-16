# Proposal for 2  Modes of Operation (slated for implementation)

* * *
## Normal Mode 
   - Watches S3 buckets with a Manifest file that match the key pattern and data pattern.
   - Performs upsert on new data when found.
   - *Deletes* S3 files from bucket that were upserted into Redshift.
   - Keeps watching the S3 bucket as per the Manifest file in the bucket.


Example **config** file (Normal mode)
```clojure
{:s3 {:credentials   {:access-key "***"
                      :secret-key "***"}
      :bucket        "your-bucket"
      :key-pattern   ".*"
      :poll-interval {:seconds 30}
      :server-side-encryption "AES256"    ;; optional
      }
 :telemetry {:reporters [uswitch.blueshift.telemetry/log-metrics-reporter]}
 :redshift-connections [{:dw1 {:jdbc-url "jdbc:postgresql://foobar...."}}
                        {:dw2 {:jdbc-url "jdbc:postgresql://blahblah.fake.com..."}}
                        ]
 }
```

Example **manifest.edn** file (Normal mode)
```clojure
{:table "mydata_fact"
 :pk-columns ["id" "blah"]
 :columns ["id" "blah" "timestamp" ]
 :database :dw1    ;; must match the config to resolve the jdbc-url
 :options      ["DELIMITER '\\t'" "IGNOREHEADER 1" "GZIP" "TRIMBLANKS" "TRUNCATECOLUMNS"]
 :data-pattern ".*blah.*.gz$"
 :keep-data-pattern-files-on-import false     
 :keep-manifest-upon-import true
 }
```


* * *
## Alternate Mode
   - Watches S3 buckets with a Manifest file that match the key pattern and data pattern.
   - Performs upsert on new data when found.
   - Does not delete S3 file data.
   - If the data loads successfully, *deletes* the Blueshift manifest file in the S3 bucket to stop watching the bucket.


Example **config** file (Alternate mode)
```clojure
{:s3 {:credentials   {:access-key "***"
                      :secret-key "***"}
      :bucket        "your-bucket"
      :key-pattern   ".*"
      :poll-interval {:seconds 30}
      :server-side-encryption "AES256"       ;; optional
      }
 :telemetry {:reporters [uswitch.blueshift.telemetry/log-metrics-reporter]}
 :redshift-connections [{:dw1 {:jdbc-url "jdbc:postgresql://foobar...."}}
                        {:dw2 {:jdbc-url "jdbc:postgresql://blahblah.fake.com..."}}
                        ]
 }
```

Example **manifest.edn** file (Alternate mode)
```clojure
{:table "mydata_fact"
 :pk-columns ["id" "blah"]
 :columns ["id" "blah" "timestamp" ]
 :database :dw1    ;; must match the config to resolve the jdbc-url
 :options      ["DELIMITER '\\t'" "IGNOREHEADER 1" "GZIP" "TRIMBLANKS" "TRUNCATECOLUMNS"]
 :data-pattern ".*blah.*.gz$"
 :keep-data-pattern-files-on-import true
 :keep-manifest-upon-import false
 }
```

