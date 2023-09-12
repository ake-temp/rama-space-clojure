(ns rama-space.extract
  (:import
    (com.rpl.rama.ops RamaFunction1)))

(deftype UserIdExtract []
  RamaFunction1
  (invoke [_ data]
    (:user-id data)))

(deftype UserId1Extract []
  RamaFunction1
  (invoke [_ data]
    (:user-id-1 data)))

(deftype ToUserIdExtract []
  RamaFunction1
  (invoke [_ data]
    (:to-user-id data)))
