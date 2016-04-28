(ns smx.cassandra.component
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [qbits.alia :as alia]
            [qbits.alia.policy.retry :as retry]
            [qbits.alia.policy.load-balancing :as balance]
            [com.stuartsierra.component :as cp]
            [schema.core :as s])
  (:import [clojure.lang Keyword IPersistentMap]
           [com.datastax.driver.core Cluster Session PreparedStatement Statement]
           [com.datastax.driver.core.policies LoadBalancingPolicy RetryPolicy]))

;;;;;;;;;;;;;
;;; Schema

(s/defschema RawQuery
  "A raw query can either be a String or a Hayt map"
  (s/either String IPersistentMap))

(s/defschema AliaExecutable
  "Alia is capable of executing these types of Statements/Queries"
  (s/either RawQuery PreparedStatement Statement))

(s/defschema Executable
  "Connection is capable of executing these types of Statements/Queries"
  (s/either Keyword AliaExecutable))

(s/defschema Vals
  "Vals may be positional (list) or named (map)"
  (s/either [s/Any] {Keyword s/Any}))

(s/defschema Command
  "A tuple of Executable and optional values to be bound"
  [(s/one Executable "Executable") (s/optional Vals "Values")])

(s/defschema Queries
  "Each connection can be configured with a map of RawQuery to prepare"
  {Keyword RawQuery})

(s/defschema Prepared
  "When a Connection starts it converts any configured RawQuery into PreparedStatement"
  {Keyword PreparedStatement})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Cluster Configuration & Lifecycle

(def ClusterConfig
  "Partial cluster configuration schema, for the full set see: alia/cluster docstring"
  {:contact-points                         [String]
   :port                                   Number
   (s/optional-key :query-options)         {(s/optional-key :fetch-size)         Number
                                            (s/optional-key :consistency)        Keyword
                                            (s/optional-key :serial-consistency) Keyword}
   (s/optional-key :retry-policy)          Keyword
   (s/optional-key :load-balancing-policy) Keyword
   s/Any                                   s/Any})

(s/defn ->retry-policy :- RetryPolicy
  "Translate a keyword into a retry policy"
  [target :- (s/maybe Keyword)]
  (let [policy (case target
                 :default (retry/default-retry-policy)
                 :fallthrough (retry/fallthrough-retry-policy)
                 :downgrading (retry/downgrading-consistency-retry-policy)
                 :logging->default (retry/logging-retry-policy (retry/default-retry-policy))
                 :logging->fallthrough (retry/logging-retry-policy (retry/fallthrough-retry-policy))
                 :logging->downgrading (retry/logging-retry-policy (retry/downgrading-consistency-retry-policy))
                 (retry/default-retry-policy))]
    (log/info "retry-policy" target "=>" policy)
    policy))

(s/defn ->load-balancing-policy :- LoadBalancingPolicy
  "Translate a keyword into a load balancing policy, Default is token->round-robin"
  [target :- (s/maybe Keyword)]
  (let [policy (case target
                 :round-robin (balance/round-robin-policy)
                 (balance/token-aware-policy (balance/round-robin-policy)))]
    (log/info "load-balancing-policy" target "=>" policy)
    policy))

(s/defn parse-cluster-config
  "Upgrade cluster configuration map"
  [config :- ClusterConfig]
  (-> (update config :retry-policy ->retry-policy)
      (update :load-balancing-policy ->load-balancing-policy)))

(extend-protocol cp/Lifecycle
  Cluster
  (start [this]
    (log/info "initializing cluster")
    (.init this))
  (stop [this]
    (log/info "stopping cluster")
    (.close this)
    (log/info "cluster stopped")
    this))

;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Prepared Statements

(s/defn ->prepared :- Prepared
  "Upgrade each RawQuery to a PreparedStatement"
  [session :- Session
   queries :- Queries]
  (reduce (fn [ret [k v]]
            (log/info "preparing" k v)
            (assoc ret k (alia/prepare session v))) {}
          queries))

(s/defn prepared :- (s/maybe PreparedStatement)
  "Retrieve a prepared statement from the Connection"
  [connection :- Connection
   key :- Keyword]
  (get-in connection [:prepared key]))

(s/defn prepared? :- Boolean
  "Has this connection prepared all statements described by queries?"
  [connection :- Connection
   queries :- Queries]
  (every? #(prepared connection %) (keys queries)))

;;;;;;;;;;;;;;;;
;;; Component

(s/defrecord Connection [keyspace :- String
                         queries :- (s/maybe Queries)
                         prepared :- (s/maybe Prepared)
                         session :- (s/maybe Session)
                         cluster :- (s/maybe Cluster)
                         default-fetch-size :- (s/maybe Number)]
  cp/Lifecycle
  (start [this]
    (assert (and cluster keyspace) "cluster and/or keyspace cannot be nil")
    (let [session            (alia/connect cluster keyspace)
          default-fetch-size (-> session
                                 .getCluster
                                 .getConfiguration
                                 .getQueryOptions
                                 .getFetchSize)]
      (log/info "initializing connection with default-fetch-size" default-fetch-size)
      (assoc this :session session
                  :prepared (->prepared session queries)
                  :default-fetch-size default-fetch-size)))
  (stop [this]
    (log/info "closing connection")
    (alia/shutdown (:session this))
    (log/info "connection closed")
    this))

;;;;;;;;;;;;;
;;; Public

(s/defn fetch-size :- Number
  "Get the fetch-size from opts, or default fetch size from connection"
  ([connection :- Connection]
    (:default-fetch-size connection))
  ([connection :- Connection
    opts]
    (or (:fetch-size opts) (:default-fetch-size connection))))

(s/defn execute
  ([connection executable]
    (execute connection executable nil))
  ([connection :- Connection
    executable :- Executable
    opts]
    (log/trace "executing" executable opts)
    (if-let [executable (if (keyword? executable) (prepared connection executable) executable)]
      ;; Note: alia/execute-chan-buffered because of https://github.com/mpenet/alia/issues/29
      (alia/execute-chan-buffered (:session connection) executable opts)
      (let [err-chan (or (:channel opts) (async/chan 1))]
        (async/put! err-chan (ex-info "invalid executable" {:executable executable}))
        (async/close! err-chan)
        err-chan))))

(s/defn execute-batch
  ([connection commands]
    (execute-batch connection commands :logged))
  ([connection commands batch-type]
    (execute-batch connection commands batch-type nil))
  ([connection :- Connection
    commands :- [Command]
    batch-type :- Keyword
    opts]
    (try
      (log/trace "executing batch" commands opts)
      (alia/execute-chan-buffered
        (:session connection)
        (alia/batch (map (fn [[executable vals]]
                           (if-let [executable (if (keyword? executable) (prepared connection executable) executable)]
                             (alia/query->statement executable vals)
                             (throw (ex-info "invalid executable" {:executable executable}))))
                         commands) batch-type)
        opts)
      (catch Throwable thr
        (let [err-chan (or (:channel opts) (async/chan 1))]
          (async/put! err-chan thr)
          (async/close! err-chan)
          err-chan)))))

;;;;;;;;;;;;;;;;;;;;;
;;; Initialization

(s/defn ->cluster :- Cluster
  "Create a new cassandra cluster"
  [config :- ClusterConfig]
  (alia/cluster (parse-cluster-config config)))

(s/defn ->connection :- Connection
  "Create a new cassandra connection
    - keyspace: the cassandra keyspace to connect to
    - queries: a map of keyword to string or hayt query to be prepared and made available

   Results returned on a core.async channel, exceptions may be placed on the channel at any point"
  ([keyspace]
    (->connection keyspace nil))
  ([keyspace :- String
    queries :- (s/maybe Queries)]
    (map->Connection {:keyspace keyspace
                      :queries  queries})))