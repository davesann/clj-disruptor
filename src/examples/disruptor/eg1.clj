(ns examples.disruptor.eg1
  "Example using distruptor"
  (:require [disruptor.core :as dc]
            [disruptor.event :as de]
            [disruptor.events :as des])
  )

(defn timestamp []
  (System/currentTimeMillis))


;; # global state to track number of handles.  
;; This could be the application state or model    
;; depending on how the event handlers are configured,
;; this may not need th be thread safe
(def handle-count (atom 0))

(defn inc-handle-count []
  (swap! handle-count inc))

(defn reset-count [handle-count value]
  (reset! handle-count value))


;; # event handlers
;; note - return values are ignored unless the handler is registered with a 'slot'
;; (see below)

(defn handler-A [event sequence-num end-of-batch?]
  (inc-handle-count)
  {:A (timestamp)})

(defn handler-B [event sequence-num end-of-batch?]
  (inc-handle-count)
  {:B (timestamp)})

(defn handler-C [event sequence-num end-of-batch?]
  (inc-handle-count)
  {:C (timestamp)})

(defn handler-D [event sequence-num end-of-batch?]
  (inc-handle-count)
  {:D (timestamp)})

;; this is used to terminate the disruptor and print the final event  
;; in practice you would likely not do this.
(defn make-done-handler [iterations disruptor start-time]
  (fn [event sequence-num end-of-batch?]
    (inc-handle-count)
    (when (>= sequence-num iterations)
      (println "stopping")
      (.start (java.lang.Thread. #(dc/shutdown disruptor))) 
      
      (let [hc @handle-count
            t (- (timestamp) start-time)
            handles-per-second (/ (* hc 1000.0) t)]
        (println)
        (println (des/pstr event))
        (println)
        (println (format "%d iterations, %d handles, handles/s: %.0f, time(s) %.2f" 
                           iterations
                           hc
                           handles-per-second
                           (/ t 1000.0)
                           ))
        ))))

;; # General setup

;; Use a Map for the event type the slot-keys can be any valid map key
(comment
  (def create-event des/identity-map-event-factory)
  ) 

;; Use an object-array for the event type the slot-keys must be ints 
;; within the length of the array
(def create-event (des/create-object-array-event-factory 5))


(defn create-executor-service-p 
  "Tracks the number of threads created this was just for debugging"
  []
  (let [t-count (atom 0)
        thread-factory (proxy [java.util.concurrent.ThreadFactory] []
                         (newThread [^Runnable runnable] 
                                    (do
                                      (println "New thread: " (swap! t-count inc))
                                      (java.lang.Thread. runnable))))]
    (java.util.concurrent.Executors/newCachedThreadPool thread-factory)  
    ))

(defn create-executor-service 
  "Simple Cached Thread Pool"
  []
  (java.util.concurrent.Executors/newCachedThreadPool))


;; # run the example 

(def default-ring-size (* 8 1024))
(def default-num-event-processors 4)

(import '[com.lmax.disruptor RingBuffer])
(defn publish-events 
  "publish a bunch of events to the ring buffer"
  [^RingBuffer ring-buffer iterations]
  (loop [i 0]
      (when-not (> i iterations)
        (dc/publish ring-buffer 0 {:i i :start (timestamp)})
        (recur (unchecked-add 1 i))
        )))



(defn go 
  "run the example"
  ([iterations] (go iterations default-ring-size default-num-event-processors))
  ([iterations ring-size num-event-processors]
    (println "Started")
    (reset-count handle-count 0)
    (let [executor-service (create-executor-service)
          disruptor (dc/make-disruptor executor-service
                                       ring-size create-event 
                                       :single-threaded :blocking
                                       )]
      (-> 
        (des/handle-events-with disruptor {1 handler-A})
        (des/then-handle {2 handler-B 3 handler-C})
        (des/then-handle {4 handler-D})
        (des/then-handle (make-done-handler iterations disruptor (timestamp)))
        ) 
      (let [ring-buffer (dc/start disruptor)]  
        (.start (java.lang.Thread. #(publish-events ring-buffer iterations))))
      
      )))

(comment
  (def iterations 300000000)
  
  (def iterations 300000)
  (def iterations 10)
  (go iterations)
  )

    
   