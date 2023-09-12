(ns word-count 
  (:require [rama.block :as block]
            [rama.compound-agg :as compound-agg])
  (:import
    (com.rpl.rama Depot RamaModule PState Agg Path)
    (com.rpl.rama.ops Ops)))

(deftype SimpleWordCountsModule []
  RamaModule
  (define [_ setup topologies]
    (.declareDepot setup "*depot" (Depot/random))
    (let [s (.stream topologies "s")]
      (.pstate s "$$word-counts" (PState/mapSchema String Long))
      (-> (.source s "*depot")
          (block/out "*token")
          (.hashPartition "*token")
          (.each Ops/PRINTLN "received the word: " "*token")
          (.compoundAgg "$$word-counts"
                        (compound-agg/map
                          "*token" (Agg/count)))))))

(comment
  ;; Start a local (test) cluster
  (import '(com.rpl.rama.test InProcessCluster))
  (def cluster (InProcessCluster/create))
  (def simple-word-counts-module-name
    (.getName SimpleWordCountsModule))

  ;; Launch a module into the cluster
  (require '[rama.core :as rama])
  (rama/run (SimpleWordCountsModule.) cluster)
  (rama/stop simple-word-counts-module-name cluster)

  ;; Append some data to the depot
  (let [depot (.clusterDepot cluster simple-word-counts-module-name "*depot")]
    (.append depot "one")
    (.append depot "two")
    (.append depot "two")
    (.append depot "three")
    (.append depot "three")
    (.append depot "three"))

  (let [wc (.clusterPState cluster simple-word-counts-module-name "$$word-counts")]
    (println "Word counts: " (.selectOne wc (Path/key (into-array String ["zero"]))))
    (println "Word counts: " (.selectOne wc (Path/key (into-array String ["one"]))))
    (println "Word counts: " (.selectOne wc (Path/key (into-array String ["two"]))))
    (println "Word counts: " (.selectOne wc (Path/key (into-array String ["three"]))))))
