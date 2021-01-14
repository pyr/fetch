(ns fetch.grpc.context
  (:import io.grpc.Context
           io.grpc.Context$Key
           io.grpc.Contexts
           io.grpc.Metadata
           io.grpc.ServerCall
           io.grpc.ServerCallHandler))

(def ^Context$Key engine-key
  "Key the prefixed storage engine will live in"
  (Context/key "cluster-id"))

(defn engine
  "Retrieve engine from the context"
  []
  (.get engine-key))

(defn ^Context with-engine
  "Add engine to the current context"
  [engine]
  (.withValue (Context/current) engine-key engine))

(defn intercept
  [^Context ctx ^ServerCall call ^Metadata headers ^ServerCallHandler next]
  (Contexts/interceptCall ctx call headers next))
