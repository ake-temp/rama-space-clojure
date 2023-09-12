(ns rama-space.module-test
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]

    [rama-space.client :as c]
    [rama-space.module])
  (:import
    (rama_space.module RamaSpaceModule)
    (com.rpl.rama.test InProcessCluster LaunchConfig)))

(def ipc (atom nil))
(def module-name (.getName RamaSpaceModule))

(use-fixtures :each
  (fn [f]
    (let [module (RamaSpaceModule.)
          cluster (InProcessCluster/create)]
      (try
        (.launchModule cluster module (LaunchConfig. 4 4))
        (reset! ipc cluster)
        (f)
        (finally
          (reset! ipc nil)
          (.destroyModule cluster module-name)
          (.close cluster))))))

(deftest basic-test
  (let [cluster @ipc
        client (c/make-client cluster)]
    (testing "Register user"
      (is (true? (c/append-user-registration client
                   {:user-id "alice"
                    :email "alice@gmail.com"
                    :display-name "Alice Alice"
                    :pwd-hash 1})))
      (is (false? (c/append-user-registration client
                    {:user-id "alice"
                     :email "alice2@gmail.com"
                     :display-name "Alice"
                     :pwd-hash 2})))
      (is (= 1 (c/pwd-hash client "alice"))))

    (testing "Edit user profile"
      (c/append-profile-edit client "alice" "bio" "in wonderland")
      (let [profile (c/profile client "alice")]
        (is (= "Alice Alice" (:display-name profile)))
        (is (= "alice@gmail.com" (:email profile)))
        (is (= "in wonderland" (:bio profile)))
        (is (> (:joined-at-millis profile) 0))
        (is (nil? (:location profile)))))

    (testing "resolving posts from multiple users"
      (is (true? (c/append-user-registration client
                   {:user-id "bob"
                    :email "bob@gmail.com"
                    :display-name "Bobby"
                    :pwd-hash 2})))
      (is (true? (c/append-user-registration client
                   {:user-id "charlie"
                    :email "charlie@gmail.com"
                    :display-name "Charles"
                    :pwd-hash 2})))
      (doseq [i (range 8)]
        (c/append-post client "alice" "alice" (str "x " i))
        (c/append-post client "bob" "alice" (str "y " i))
        (c/append-post client "charlie" "alice" (str "z " i)))
      (.waitForMicrobatchProcessedCount cluster module-name "posts" 24)

      (is (= 24 (c/posts-count client "alice")))

      (let [page-1 (c/resolve-posts client "alice")]
        (is (= 20 (count page-1)))
        (let [[last-id _] (last page-1)
              page-2 (c/resolve-posts client "alice" (inc last-id))]
          (is (= 4 (count page-2))))))

    (testing "friendships"
      (testing "users start with no friends"
        (is (= 0 (c/friend-count client "alice")))
        (is (= 0 (c/friend-count client "bob")))
        (is (= 0 (c/friend-count client "charlie"))))

      (c/append-friend-request client "alice" "bob")
      (c/append-friend-request client "alice" "charlie")

      (testing "users have no friends until they accept them"
        (is (= 0 (c/friend-count client "alice")))
        (is (= 0 (c/friend-count client "bob")))
        (is (= 0 (c/friend-count client "charlie"))))

      (testing "incoming & outgoing friend requests"
        (is (empty? (c/incoming-friend-requests client "alice")))
        (let [incoming (c/incoming-friend-requests client "bob")]
          (is (= 1 (count incoming)))
          (is (= "alice" (first incoming))))
        (let [incoming (c/incoming-friend-requests client "charlie")]
          (is (= 1 (count incoming)))
          (is (= "alice" (first incoming))))

        (let [requests (c/outgoing-friend-requests client "alice")]
          (is (= 2 (count requests)))
          (is (= "bob" (first requests)))
          (is (= "charlie" (second requests)))))

      (c/append-cancel-friend-request client "alice" "bob")

      (testing "cancelling a request"
        (is (empty? (c/incoming-friend-requests client "bob"))))

      (c/append-friendship-add client "alice" "bob")
      (c/append-friendship-add client "alice" "charlie")

      (testing "adding a friendship"
        (is (= 2 (c/friend-count client "alice")))
        (is (= 1 (c/friend-count client "bob")))
        (is (= 1 (c/friend-count client "charlie")))

        (is (true? (c/friends? client "alice" "bob")))
        (is (true? (c/friends? client "bob" "alice")))
        (is (true? (c/friends? client "alice" "charlie")))
        (is (false? (c/friends? client "bob" "charlie")))

        (is (= (c/friends-page client "alice")
               #{"bob" "charlie"}))))

    (testing "profile views"
      (is (= 0 (c/num-profile-views client "alice")))

      (let [bucket-denominator (* 60 60 1000)]
        (c/append-profile-view client "alice" 0)
        (c/append-profile-view client "alice" (* 2 bucket-denominator))
        (c/append-profile-view client "alice" (* 4 bucket-denominator))
        (c/append-profile-view client "bob" 0))
      (.waitForMicrobatchProcessedCount cluster module-name "profile-views" 4)

      (is (= 3 (c/num-profile-views client "alice")))
      (is (= 1 (c/num-profile-views client "alice" 0 2)) "non inclusive")
      (is (= 2 (c/num-profile-views client "alice" 0 3)))
      (is (= 1 (c/num-profile-views client "alice" 1 3))))))
