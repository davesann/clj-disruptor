(ns disruptor.events
  "Event Handler creation and event sequencing"
  (:import [com.lmax.disruptor.dsl Disruptor]
           [com.lmax.disruptor 
            EventFactory 
            EventHandler]
           )
  (:require [disruptor.event :as de]
            [clojure.string :as s])
  )


;; # internals

(defn make-event-handler 
  "Make an event handler

   If slot-id is provided, then when the handler returns a non nil result 
   it will be inserted into the specified event slot 
  "
  ([handle-event] 
    (proxy [EventHandler] []
      (onEvent [event sequence-num end-of-batch?]
               (handle-event event sequence-num end-of-batch?))))
  ([handle-event slot-id]
    (proxy [EventHandler] []
      (onEvent [event sequence-num end-of-batch?]
               (when-let [r (handle-event event sequence-num end-of-batch?)]
                 (de/set-slot! event slot-id r))))))

(defprotocol AsEventHandler
  "Support conversions to event handler"
  (as-event-handler 
    [this] 
    [this slot-key]
    ))

(extend-protocol AsEventHandler
  ;; nop - ensures that mixure of fns and event handlers is ok.
  EventHandler 
  (as-event-handler 
    ([this] this)
    ([this slot-key] this))
  
  clojure.lang.IFn
  (as-event-handler 
    ([f]          (make-event-handler f))
    ([f slot-key] (make-event-handler f slot-key)))
  )


(defn to-handler-array 
  "Convert a fn, seq of (fns or event-handlers) or map

     {slot-id handler  
      slot-id2 handler2}

   to an array of event handlers"
  [fns]
  (cond 
    (fn? fns)
    (into-array EventHandler [(as-event-handler fns)])
    
    (sequential? fns)
    (into-array EventHandler (map as-event-handler fns))
    
    (map? fns)
      (into-array EventHandler 
                  (map (fn [[slot-key handler]] 
                         (as-event-handler handler slot-key)) 
                       (seq fns)))
    ))


;; # sequencing event handlers
;; These functions are used to place handlers is sequence or parallel
;; 
;; example:
;;      <pre><code>(-> disruptor   
;;        (handle-events-with f1)  
;;        (then-handle [f2 f3])  
;;        (and-handle  {:slot1 f4 :slot2 f5})  
;;        (then-handle [final-one]))</code></pre>
;; is:  
;;  - publish  
;;  - then f1  
;;  - then f2, f3, f4 and f5 in parallel,  
;;  - f4 and f5 will populate slots :slot1 and :slot2 in the event  
;;  - then final-one  
 

(defn handle-events-with 
  "add fns as direct handlers of published events on the disruptor (ring-buffer)"
  [disruptor fns]
  (.handleEventsWith disruptor (to-handler-array fns)))

(defn then-handle
  "add fns as handlers processing events in sequence following the supplied handler group"
  [handler-group fns]
  (.then handler-group (to-handler-array fns)))
    
(defn and-handle 
  "add fns as handlers processing events in parallel with the supplied handler group"
  [handler-group fns]
  (.and handler-group (to-handler-array fns)))

(defn after 
  "add fns as handlers processing events in sequence with the supplied handler group"
  [handler-group disruptor fns]
  (.after disruptor handler-group (to-handler-array fns)))



;; # event factories
;; Event factories are used by the ring buffer to create events 
;; The Java implemetations usually usea  class directly and get/set fields.
;; for flexibility Map and Arrays are used here

(defn identity-map-event-factory 
  "use this as the create-event-fn if you want to use a Map for the 
   Event in the RingBuffer"
  []
  (java.util.IdentityHashMap.))

(defn create-object-array-event-factory [size]
  "use this ato create the create-event-fn if you want to use an object array 
   for the Event in the RingBuffer"
  (fn [] (object-array size)))


;; # print events
(defprotocol EventToStr
  "protocol to mainly support printing java array - debugging essentially"
  (pstr [event]))

(extend-protocol EventToStr
  java.util.Map
  (pstr [event] (str event))
  
   #=(java.lang.Class/forName "[Ljava.lang.Object;")
  (pstr [event] (str  (vec event))) 
  )
