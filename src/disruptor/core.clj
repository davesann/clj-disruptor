(ns disruptor.core
  "create, start, shutdown, disruptors"
  (:require [disruptor.event :as de])
  (:import 
    [com.lmax.disruptor.dsl Disruptor]
          
    [com.lmax.disruptor 
     EventFactory 
     EventHandler
     
     SingleThreadedClaimStrategy
     MultiThreadedClaimStrategy
     
     BlockingWaitStrategy
     BusySpinWaitStrategy
     SleepingWaitStrategy
     YieldingWaitStrategy
     
     RingBuffer
     ]
    ))

(set! *warn-on-reflection* true)

(defn make-event-factory [create-event]
  (proxy [EventFactory] []
    (newInstance [] (create-event))))

(defn make-claim-strategy
  ([ring-size] (make-claim-strategy ring-size nil))
  ([ring-size claim-type]
    (case claim-type 
      :multi-threaded (MultiThreadedClaimStrategy. ring-size)
      :single-threaded (SingleThreadedClaimStrategy. ring-size)
      )))

(defn make-wait-strategy 
  ([] (make-wait-strategy nil))
  ([strategy] 
    (case strategy
      :blocking  (BlockingWaitStrategy.)
      :busy-spin (BusySpinWaitStrategy.)
      :sleeping  (SleepingWaitStrategy.)
      :yielding  (YieldingWaitStrategy.)
      )))

  "Generates an uberdoc html file from 3 pieces of information:

   2. The path to spit the result (`output-file-name`)
   1. Results from processing source files (`path-to-doc`)
   3. Project metadata as a map, containing at a minimum the following:
     - :name
     - :version
  "  


(defn make-disruptor
  "Make a disruptor:
  
   1. executor-service is a java.util.concurrent.Executor
   2. ring-size must be a power of 2
   3. create-event-fn will be used by the ring buffer to create events (see events.clj)  
   4. claim-strategy is:
     - :single-threaded - if you have one concurrent publisher
     - :multi-threaded  - if you have more
   5. wait strategy is one of:
     - :blocking
     - :busy-spin
     - :sleeping
     - :yielding
  "

  
  ([executor-service ring-size create-event-fn] 
    (make-disruptor executor-service ring-size create-event-fn :single-threaded :sleeping))  
  ([executor-service ring-size create-event-fn claim-strategy wait-strategy]
    (Disruptor.
      (make-event-factory create-event-fn)
      executor-service
      (make-claim-strategy ring-size claim-strategy)
      (make-wait-strategy  wait-strategy))))


(defn start 
  "Start the disruptor - returns the ring buffer"
  ^RingBuffer [^Disruptor disruptor]
  (.start disruptor))

(defn shutdown 
  "shutdown the disruptor
   - will shutdown at a clean point - not in the middle of an event."
  [^Disruptor disruptor]
  (.shutdown disruptor))

(defn get-event 
  "get the event at the given sequence-num"
  [^RingBuffer ring-buffer sequence-num]
  (.get ring-buffer sequence-num))

(defn claim-next 
   "claim the next sequence-num for publishing"
  ^long [^RingBuffer ring-buffer]
  (.next ring-buffer))

(defn publish 
  "publish data to the ring buffer in slot-key of the next available event" 
  [^RingBuffer ring-buffer slot-key data]
  (let [sequence-num (claim-next ring-buffer)
        event (get-event ring-buffer sequence-num)]
    (de/set-slot! event slot-key data)
    (.publish ring-buffer sequence-num)
    ))

(defn get-cursor [^RingBuffer ring-buffer]
  (.getCursor ring-buffer))


