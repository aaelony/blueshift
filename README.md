# Blueshift


Service to watch Amazon S3 and automate the load into Amazon Redshift.

![Gravitational Blueshift](http://upload.wikimedia.org/wikipedia/commons/5/5c/Gravitional_well.jpg) ([Image used under CC Attribution Share-Alike License](http://en.wikipedia.org/wiki/File:Gravitional_well.jpg)).

## Rationale

[Amazon Redshift](https://aws.amazon.com/redshift/) is a "a fast, fully managed, petabyte-scale data warehouse service" but importing data into it can be a bit tricky: e.g. if you want upsert behaviour you have to [implement it yourself with temporary tables](http://docs.aws.amazon.com/redshift/latest/dg/t_updating-inserting-using-staging-tables-.html), and we've had problems [importing across machines into the same tables](https://forums.aws.amazon.com/message.jspa?messageID=443795). Redshift also performs best when bulk importing lots of large files from S3.

[Blueshift](https://github.com/uswitch/blueshift) is a little service(tm) that makes it easy to automate the loading of data from Amazon S3 and into Amazon Redshift. It will periodically check for data files within a designated bucket and, when new files are found, import them. It provides upsert behaviour by default. 

Importing to Redshift now requires just the ability to write files to S3.

## Using

### Redshift driver
lein localrepo install drivers/RedshiftJDBC42-1.2.1.1001.jar redshift/jdbc "42.1.2.1.1001"


### Configuring

Blueshift requires minimal configuration. It will only monitor a single S3 bucket currently, so the configuration file (ordinarily stored in `./etc/config.edn`) looks like this:

```clojure
{:s3 {:credentials   {:access-key ""
                      :secret-key ""}
      :bucket        "blueshift-data"
      :key-pattern   ".*"
      :poll-interval {:seconds 30}
      :server-side-encryption "AES256"
      :keep-s3-files-on-import true
	}
 :telemetry {:reporters [uswitch.blueshift.telemetry/log-metrics-reporter]}
 :redshift-connections [{:dw1 {:jdbc-url "jdbc:postgresql://foo.eu-west-1.redshift.amazonaws.com:5439/db?tcpKeepAlive=true&user=user&password=pwd"}}
                        {:dw2 {:jdbc-url "jdbc:postgresql://blahblah.us-east-1.redshift.amazonaws.com:5439/db?tcpKeepAlive=true&user=user&password=pwd"}} ]
}
```

The S3 credentials are shared by Blueshift for watching for new files and for [Redshift's `COPY` command](http://docs.aws.amazon.com/redshift/latest/dg/t_loading-tables-from-s3.html). The `:key-pattern` option is used to filter for specific keys (so you can have a single bucket with data from different environments, systems etc.).

The :redshift-connections key contains a vector of database keys with maps pointing to redshift jdbc urls.  If the manifest in the s3 bucket contains a :database key with a value keyword matching a key in the :redshift-connections vector, that will allow Blueshift to lookup the jdbc-url from the specified database key.  In the above example, :dw1 would match with :database :dw1 in the manifest file.


### Building & Running

The application is written in [Clojure](http://clojure.org), to build the project you'll need to use [Leiningen](https://github.com/technomancy/leiningen).

If you want to run the application on your computer you can run it directly with Leiningen (providing the path to your configuration file)

    $ lein run -- --config ./etc/config.edn

Alternatively, you can build an Uberjar that you can run:

    $ lein uberjar
    $ java -Dlogback.configurationFile=./etc/logback.xml -jar target/blueshift-0.1.0-standalone.jar --config ./etc/config.edn

The uberjar includes [Logback](http://logback.qos.ch/) for logging. `./etc/logback.xml.example` provides a simple starter configuration file with a console appender. 

## Using

Once the service is running you can create any number of directories in the S3 bucket. These will be periodically checked for files and, if found, an import triggered. If you wish the contents of the directory to be imported it's necessary for it to contain a file called `manifest.edn` which is used by Blueshift to know which Redshift cluster to import to and how to interpret the data files.

Your S3 structure could look like this:

      bucket
      ├── directory-a
      │   └── foo
      │       └── manifest.edn
      │       └── 0001.tsv
      │       └── 0002.tsv
      └── directory-b
          └── manifest.edn

and the `manifest.edn` could look like this:

    {:table        "testing"
     :pk-columns   ["foo"]
     :columns      ["foo" "bar"]
     :database     :dw1
     :options      ["DELIMITER '\\t'" "IGNOREHEADER 1" "ESCAPE" "TRIMBLANKS"]
     :data-pattern ".*tsv$"}

When a manifest and data files are found an import is triggered. Once the import has been successfully committed Blueshift will **delete** any data files that were imported; the manifest remains ready for new data files to be  imported.

It's important that `:columns` lists all the columns (and only the columns) included within the data file and that they are in the same order. `:pk-columns` must contain a uniquely identifying primary key to ensure the correct upsert behaviour. `:options` can be used to override the Redshift copy options used during the load.

Blueshift creates a temporary Amazon Redshift Copy manifest that lists all the data files found as mandatory for importing, this also makes it very efficient when loading lots of files into a highly distributed cluster.

## Metrics
Blueshift tracks a few metrics using [https://github.com/sjl/metrics-clojure](https://github.com/sjl/metrics-clojure). Currently these are logged to the Slf4j logger.

Starting the app will (eventually) show something like this:

    [metrics-logger-reporter-thread-1] INFO user - type=COUNTER, name=uswitch.blueshift.s3.directories-watched.directories, count=0
    [metrics-logger-reporter-thread-1] INFO user - type=METER, name=uswitch.blueshift.redshift.redshift-imports.commits, count=0, mean_rate=0.0, m1=0.0, m5=0.0, m15=0.0, rate_unit=events/second
    [metrics-logger-reporter-thread-1] INFO user - type=METER, name=uswitch.blueshift.redshift.redshift-imports.imports, count=0, mean_rate=0.0, m1=0.0, m5=0.0, m15=0.0, rate_unit=events/second
    [metrics-logger-reporter-thread-1] INFO user - type=METER, name=uswitch.blueshift.redshift.redshift-imports.rollbacks, count=0, mean_rate=0.0, m1=0.0, m5=0.0, m15=0.0, rate_unit=events/second

### Riemann Metrics
Reporting metrics to [Riemann](http://riemann.io/) can be achieved using the [https://github.com/uswitch/blueshift-riemann-metrics](https://github.com/uswitch/blueshift-riemann-metrics) project. To enable support you'll need to build the project:

    $ cd blueshift-riemann-metrics
    $ lein uberjar

And then change the `./etc/config.edn` to reference the riemann reporter:

    :telemetry {:reporters [uswitch.blueshift.telemetry/log-metrics-reporter
                            uswitch.blueshift.telemetry.riemann/riemann-metrics-reporter]}

Then, when you've built and run Blueshift, be sure to add the jar to the classpath (the following assumes you're in the blueshift working directory):

    $ cp blueshift-riemann-metrics/target/blueshift-riemann-metrics-0.1.0-standalone.jar ./target
    $ java -cp "target/*" uswitch.blueshift.main --config ./etc/config.edn

Obviously for a production deployment you'd probably want to automate this with your continuous integration server of choice :)

## TODO

* Add exception handling when cleaning uploaded files from S3
* Change `KeyWatcher` to identify when directories are deleted, can exit the watcher process and remove from the list of watched directories. If it's added again later can then just create a new process.
* Add safety check when processing data files- ensure that the header line of the TSV file matches the contents of `manifest.edn`

## Authors

* [Paul Ingles](https://github.com/pingles) ([@pingles](http://twitter.com/pingles))
* [Thomas Kristensen](https://github.com/tgk) ([@tgkristensen](http://twitter.com/tgkristensen))

## License

Copyright © 2014 [uSwitch.com](http://www.uswitch.com) Limited.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

