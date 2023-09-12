(ns rama.block
  (:import
    (com.rpl.rama SubSource)))

(defn out [block & args]
  (.out block (into-array String args)))

(defn sub-source [block arg & sub-sources]
  (.subSource block arg (into-array SubSource sub-sources)))
