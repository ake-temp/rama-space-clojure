(ns rama-space.view
  (:require [clojure.walk :refer [keywordize-keys]])
  (:import
    (com.rpl.rama.ops RamaFunction1)))

(deftype KeywordizeKeys []
  RamaFunction1
  (invoke [_ data]
    (keywordize-keys data)))
