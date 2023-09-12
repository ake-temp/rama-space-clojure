(ns rama.path
  (:refer-clojure :exclude [key])
  (:import
    (com.rpl.rama Path)))

(defn key [& args]
  (Path/key (into-array Object args)))

(defn multi-path [path & paths]
  (.multiPath path (into-array Path paths)))

(defn sub-map [path & paths]
  (.subMap path (into-array Object paths)))
