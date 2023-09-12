(ns rama-space.module
  (:require [rama.core :as rama]
            [rama.path :as path]
            [rama.block :as block]
            [rama.compound-agg :as compound-agg]
            [rama_space.extract])
  (:import
    (rama_space.extract UserIdExtract UserId1Extract ToUserIdExtract)
    (com.rpl.rama Block Depot Expr RamaModule PState Agg SubSource)
    (com.rpl.rama.ops Ops RamaFunction1)
    (com.rpl.rama.helpers TaskUniqueIdPState)))

(defn declare-users-topology [topologies]
  (let [users (.stream topologies "users")]
    (.pstate users "$$profiles"
      (PState/mapSchema
        String
        (PState/fixedKeysSchema
          (into-array
            Object
            ["display-name" String
             "email" String
             "profile-pic" String
             "bio" String
             "location" String
             "pwd-hash" Integer
             "joined-at-millis" Long
             "registration-uuid" String]))))

    (-> (.source users "*userRegistrationsDepot")
        (block/out "*registration")
        (.macro (rama/extract-map-fields "*registration"
                  #{"*user-id" "*email" "*display-name"
                    "*pwd-hash" "*registration-uuid"}))
        (.each (rama/fn->rama-function #(System/currentTimeMillis)))
        (block/out "*joined-at-millis")
        (.localTransform "$$profiles"
          (-> (path/key "*user-id")
              (.filterPred Ops/IS_NULL)
              (path/multi-path
                (-> (path/key "email") (.termVal "*email"))
                (-> (path/key "display-name") (.termVal "*display-name"))
                (-> (path/key "pwd-hash") (.termVal "*pwd-hash"))
                (-> (path/key "joined-at-millis") (.termVal "*joined-at-millis"))
                (-> (path/key "registration-uuid") (.termVal "*registration-uuid"))))))
    (-> (.source users "*profileEditsDepot")
        (block/out "*edit")
        (.macro (rama/extract-map-fields "*edit"
                  #{"*user-id" "*field" "*value"}))
        (.localTransform "$$profiles"
                         (-> (path/key "*user-id" "*field")
                             (.termVal "*value"))))))

(defrecord FriendRequest [user-id to-user-id])
(defrecord CancelFriendRequest [user-id to-user-id])
(defrecord FriendshipAdd [user-id-1 user-id-2])
(defrecord FriendshipRemove [user-id-1 user-id-2])

(defn declare-friends-topology [topologies]
  (let [friends (.stream topologies "friends")]
    (.pstate friends "$$outgoingFriendRequests"
      (PState/mapSchema
        String
        (.subindexed (PState/setSchema String))))
    (.pstate friends "$$incomingFriendRequests"
      (PState/mapSchema
        String
        (.subindexed (PState/setSchema String))))
    (.pstate friends "$$friends"
      (PState/mapSchema
        String
        (.subindexed (PState/setSchema String))))
    (-> (.source friends "*friendRequestsDepot")
        (block/out "*request")
        (.macro (rama/extract-map-fields "*request"
                  #{"*user-id" "*to-user-id"}))
        (block/sub-source "*request"
          (-> (SubSource/create FriendRequest)
              (.compoundAgg "$$outgoingFriendRequests"
                            (compound-agg/map
                              "*user-id" (Agg/set "*to-user-id")))
              (.hashPartition "*to-user-id")
              (.compoundAgg "$$incomingFriendRequests"
                            (compound-agg/map
                              "*to-user-id" (Agg/set "*user-id"))))
          (-> (SubSource/create CancelFriendRequest)
              (.compoundAgg "$$outgoingFriendRequests"
                            (compound-agg/map
                              "*user-id" (Agg/setRemove "*to-user-id")))
              (.hashPartition "*to-user-id")
              (.compoundAgg "$$incomingFriendRequests"
                            (compound-agg/map
                              "*to-user-id" (Agg/setRemove "*user-id"))))))
    (-> (.source friends "*friendshipChangesDepot")
        (block/out "*change")
        (.macro (rama/extract-map-fields "*change"
                  #{"*user-id-1" "*user-id-2"}))
        (.anchor "start")
        (.compoundAgg "$$incomingFriendRequests"
                      (compound-agg/map
                        "*user-id-1" (Agg/setRemove "*user-id-2")))
        (.compoundAgg "$$outgoingFriendRequests"
                      (compound-agg/map
                        "*user-id-1" (Agg/setRemove "*user-id-2")))
        (.hashPartition "*user-id-2")
        (.compoundAgg "$$incomingFriendRequests"
                      (compound-agg/map
                        "*user-id-2" (Agg/setRemove "*user-id-1")))
        (.compoundAgg "$$outgoingFriendRequests"
                      (compound-agg/map
                        "*user-id-2" (Agg/setRemove "*user-id-1")))
        (.hook "start")
        (block/sub-source "*change"
          (-> (SubSource/create FriendshipAdd)
              (.compoundAgg "$$friends"
                            (compound-agg/map
                              "*user-id-1" (Agg/set "*user-id-2")))
              (.hashPartition "*user-id-2")
              (.compoundAgg "$$friends"
                            (compound-agg/map
                              "*user-id-2" (Agg/set "*user-id-1"))))
          (-> (SubSource/create FriendshipRemove)
              (.compoundAgg "$$friends"
                            (compound-agg/map
                              "*user-id-1" (Agg/setRemove "*user-id-2")))
              (.hashPartition "*user-id-2")
              (.compoundAgg "$$friends"
                            (compound-agg/map
                              "*user-id-2" (Agg/setRemove "*user-id-1"))))))))

(defn declare-posts-topology [topologies]
  (let [posts (.microbatch topologies "posts")
        id (TaskUniqueIdPState. "$$postId")]
    (.pstate posts "$$posts"
      (PState/mapSchema
        String
        (.subindexed (PState/mapSchema Long clojure.lang.PersistentArrayMap))))
    (.declarePState id posts)
    (-> (.source posts "*postsDepot")
        (block/out "*microbatch")
        (.explodeMicrobatch "*microbatch")
        (block/out "*post")
        (.macro (rama/extract-map-fields "*post"
                  #{"*to-user-id"}))
        (.macro (.genId id "*id"))
        (.localTransform "$$posts"
          (-> (path/key "*to-user-id" "*id")
              (.termVal "*post"))))))

(defn declare-profile-views-topology [topologies]
  (let [profile-views (.microbatch topologies "profile-views")]
    (.pstate profile-views "$$profileViews"
      (PState/mapSchema
        String
        (.subindexed (PState/mapSchema Long Long))))
    (-> (.source profile-views "*profileViewsDepot")
        (block/out "*microbatch")
        (.explodeMicrobatch "*microbatch")
        (block/out "*profileView")
        (.macro (rama/extract-map-fields "*profileView"
                  #{"*to-user-id" "*timestamp"}))
        (.each (rama/fn->rama-function
                 (fn [timestamp]
                   (quot timestamp
                         (* 60 60 1000))))
               "*timestamp")
        (block/out "*bucket")
        (.compoundAgg "$$profileViews"
          (compound-agg/map
            "*to-user-id"
            (compound-agg/map
              "*bucket" (Agg/count)))))))

(deftype RamaSpaceModule []
  RamaModule
  (define [_ setup topologies]
    (doto setup
      ;; NOTE: For Depot/hashBy we have to provide either an aot'd Class or
      ;;       something that implements `com.rpl.rama.impl.NativeRamaFunction1`
      ;;       which we don't have access to.
      ;; NOTE: There are the same restrictions with .declareDepot
      (.declareDepot "*userRegistrationsDepot" (Depot/hashBy UserIdExtract))
      (.declareDepot "*profileEditsDepot" (Depot/hashBy UserIdExtract))
      (.declareDepot "*profileViewsDepot" (Depot/hashBy ToUserIdExtract))
      (.declareDepot "*friendRequestsDepot" (Depot/hashBy UserIdExtract))
      (.declareDepot "*friendshipChangesDepot" (Depot/hashBy UserId1Extract))
      (.declareDepot "*postsDepot" (Depot/hashBy ToUserIdExtract)))
    (declare-users-topology topologies)
    (declare-friends-topology topologies)
    (declare-posts-topology topologies)
    (declare-profile-views-topology topologies)

    (-> (.query topologies "resolvePosts" (into-array String ["*for-user-id" "*start-post-id"]))
        (block/out "*resultMap")
        (.hashPartition "*for-user-id")
        (.localSelect "$$posts"
                      (-> (path/key "*for-user-id")
                          (.sortedMapRangeFrom "*start-post-id" 20)))
        (block/out "*submap")
        (.each Ops/EXPLODE_MAP "*submap")
        (block/out "*i" "*post")
        (.macro (rama/extract-map-fields "*post"
                  #{"*user-id" "*content"}))
        (.hashPartition "*user-id")
        (.localSelect "$$profiles"
                      (path/key "*user-id" "display-name"))
        (block/out "*display-name")
        (.localSelect "$$profiles"
                      (path/key "*user-id" "profile-pic"))
        (block/out "*profile-pic")
        (.each (rama/fn->rama-function
                 (fn [user-id content display-name profile-pic]
                   {:user-id user-id
                    :content content
                    :display-name display-name
                    :profile-pic profile-pic}))
               "*user-id" "*content" "*display-name" "*profile-pic")
        (block/out "*resolvedPost")
        (.originPartition)
        (.compoundAgg
          (compound-agg/map
            "*i" (Agg/last "*resolvedPost")))
        (block/out "*resultMap"))))

(comment
  ;; Make sure the extract classes are compiled
  (compile 'rama-space.extract)

  ;; Start a local (test) cluster
  (import '(com.rpl.rama.test InProcessCluster))
  (def cluster (InProcessCluster/create))
  (def rama-space-module-name
    (.getName RamaSpaceModule))

  ;; Launch a module into the cluster
  (rama/run (RamaSpaceModule.) cluster)
  (rama/stop rama-space-module-name cluster)

  (.clusterPState cluster
                  rama-space-module-name
                  "$$friends")
  (.clusterDepot cluster
                 rama-space-module-name
                 "*depot")

  (require '[rama-space.client :as c])
  (def client (c/make-client cluster))
  (c/num-profile-views client "user-1" 0 100))
