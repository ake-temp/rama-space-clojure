(ns rama-space.client
  (:require [rama.path :as path])
  (:import
    (rama_space.module RamaSpaceModule
                       FriendRequest CancelFriendRequest
                       FriendshipAdd FriendshipRemove)
    (rama_space.view KeywordizeKeys)
    (com.rpl.rama Path SortedRangeFromOptions)
    (com.rpl.rama.ops Ops)))


;; >> Client

(defn make-client [cluster]
  (let [module-name (.getName RamaSpaceModule)
        pstate #(.clusterPState cluster module-name %)
        depot #(.clusterDepot cluster module-name %)
        query #(.clusterQuery cluster module-name %)]
    {:pstate
     {:profiles (pstate "$$profiles")
      :outgoing-friend-requests (pstate "$$outgoingFriendRequests")
      :incoming-friend-requests (pstate "$$incomingFriendRequests")
      :friends (pstate "$$friends")
      :posts (pstate "$$posts")
      :profile-views (pstate "$$profileViews")}

     :depot
     {:user-registrations (depot "*userRegistrationsDepot")
      :profile-edits (depot "*profileEditsDepot")
      :profile-views (depot "*profileViewsDepot")
      :friend-requests (depot "*friendRequestsDepot")
      :friendship-changes (depot "*friendshipChangesDepot")
      :posts (depot "*postsDepot")}

     :query
     {:resolve-posts (query "resolvePosts")}}))



;; >> Profile

(defn append-user-registration [{:keys [depot pstate] :as _client} profile]
  (let [registration-uuid (str (random-uuid))]
    (.append (:user-registrations depot)
             (assoc profile :registration-uuid registration-uuid))
    (= registration-uuid
       (.selectOne (:profiles pstate)
                   (path/key (:user-id profile) "registration-uuid")))))

(defn append-profile-edit [client user-id field value]
  (.append (get-in client [:depot :profile-edits])
           {:user-id user-id
            :field field
            :value value}))

(defn pwd-hash [client user-id]
  (.selectOne (get-in client [:pstate :profiles]) 
              (path/key user-id "pwd-hash")))

(defn profile [client user-id]
  (.selectOne (get-in client [:pstate :profiles])
              (-> (path/key user-id)
                  (path/sub-map
                    "display-name"
                    "email"
                    "profile-pic"
                    "bio"
                    "location"
                    "pwd-hash"
                    "joined-at-millis"
                    "registration-uuid")
                  (.view (KeywordizeKeys.))
                  ; NOTE: I got an exception when I tried to do this
                  ; Caused by: java.lang.ClassCastException: rama_space.client$profile$reify_998
                  #_(.view (rama/fn->rama-function keywordize-keys)))))



;; >> Friends

(defn append-friend-request [client user-id to-user-id]
  (.append (get-in client [:depot :friend-requests])
           (FriendRequest. user-id to-user-id)))

(defn append-cancel-friend-request [client user-id to-user-id]
  (.append (get-in client [:depot :friend-requests])
           (CancelFriendRequest. user-id to-user-id)))

(defn append-friendship-add [client user-id-1 user-id-2]
  (.append (get-in client [:depot :friendship-changes])
           (FriendshipAdd. user-id-1 user-id-2)))

(defn append-friendship-remove [client user-id-1 user-id-2]
  (.append (get-in client [:depot :friendship-changes])
           (FriendshipRemove. user-id-1 user-id-2)))

(defn friend-count [client user-id]
  (.selectOne (get-in client [:pstate :friends])
              (-> (path/key user-id)
                  (.view Ops/SIZE))))

(defn friends? [client user-id-1 user-id-2]
  (-> (get-in client [:pstate :friends])
      (.select (-> (path/key user-id-1)
                   (.setElem user-id-2)))
      seq
      boolean))

(defn friends-page
  ([client user-id]
   (friends-page client user-id 0))
  ([client user-id start]
   (.selectOne (get-in client [:pstate :friends])
               (-> (path/key user-id)
                   (.sortedSetRangeFrom start
                                        (-> (SortedRangeFromOptions/maxAmt 20)
                                            (.excludeStart)))))))

(defn outgoing-friend-requests
  ([client user-id]
   (outgoing-friend-requests client user-id 0))
  ([client user-id start]
   (.selectOne (get-in client [:pstate :outgoing-friend-requests])
               (-> (path/key user-id)
                   (.sortedSetRangeFrom start
                                        (-> (SortedRangeFromOptions/maxAmt 20)
                                            (.excludeStart)))))))

(defn incoming-friend-requests
  ([client user-id]
   (incoming-friend-requests client user-id 0))
  ([client user-id start]
   (.selectOne (get-in client [:pstate :incoming-friend-requests])
               (-> (path/key user-id)
                   (.sortedSetRangeFrom start
                                        (-> (SortedRangeFromOptions/maxAmt 20)
                                            (.excludeStart)))))))



;; >> Profile Views

(defn append-profile-view [client to-user-id timestamp]
  (.append (get-in client [:depot :profile-views])
           {:to-user-id to-user-id
            :timestamp (long timestamp)}))

(defn num-profile-views
  ([client user-id]
   (num-profile-views
     client user-id 0 Long/MAX_VALUE))
  ([client user-id start-hour-bucket]
   (num-profile-views
     client user-id start-hour-bucket Long/MAX_VALUE))
  ([client user-id start-hour-bucket end-hour-bucket]
   (.selectOne (get-in client [:pstate :profile-views])
               (-> (path/key user-id)
                   (.sortedMapRange start-hour-bucket end-hour-bucket)
                   (.subselect (Path/mapVals))
                   (.view Ops/SUM)))))



;; >> Posts

(defn append-post [client user-id to-user-id content]
  (.append (get-in client [:depot :posts])
           {:user-id user-id
            :to-user-id to-user-id
            :content content}))

(defn posts-count [client user-id]
  (.selectOne (get-in client [:pstate :posts])
              (-> (path/key user-id)
                  (.view Ops/SIZE))))

(defn resolve-posts
  ([client user-id]
   (resolve-posts client user-id 0))
  ([client user-id index]
   (into (sorted-map)
         (.invoke (get-in client [:query :resolve-posts])
                  (into-array Object [user-id index])))))
