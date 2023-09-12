(ns rama.core
  (:import
    (com.rpl.rama Block Helpers RamaModule)
    (com.rpl.rama.ops RamaFunction)
    (com.rpl.rama.test InProcessCluster LaunchConfig)))

(defn flatten-1 [coll]
  (->> coll
       (mapcat #(if (sequential? %) % [%]))
       vec))

(defmacro fn->rama-function [f]
  `(reify
     ~@(flatten-1
         (for [i (range 0 9)]
           (let [interface (symbol (str (.getName RamaFunction) i))
                 f-args (->> (range i)
                             (map #(->> % (str "arg") symbol))
                             vec)
                 i-args (->> f-args
                             (concat ['_])
                             vec)]
            [interface
             `(invoke ~i-args
                (~f ~@f-args))])))))

(defn extract-map-fields [from field-vars]
  (let [ret (Block/create)]
    (doseq [f field-vars]
      (let [name (if (Helpers/isGeneratedVar f)
                   (Helpers/getGeneratedVarPrefix f)
                   (subs f 1))]
        (-> ret
            (.each (-> name keyword fn->rama-function) from)
            (.out (into-array String [f])))))
    ret))

(defn run [^RamaModule module ^InProcessCluster cluster & [^LaunchConfig launch-config]]
  (.launchModule cluster module (or launch-config
                                    (LaunchConfig. 1 1))))

(defn stop [module-name ^InProcessCluster cluster]
  (.destroyModule cluster module-name))
