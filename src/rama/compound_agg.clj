(ns rama.compound-agg
  (:refer-clojure :exclude [map])
  (:import
    (com.rpl.rama CompoundAgg)))

(defn map [& args]
  (CompoundAgg/map
    (into-array Object args)))
