(ns disruptor.event
  "protocols for Disruptor events.

Note that disruptor events are _intended_ to be mutable.

The events are created in the ring-buffer.

If an event handler wants to write to the event,
either, use a dedicated 'slot', or ensure that the conflict is not possible 
via approriate sequencing.

Note also that the fastest event processing will be with a hardwired field access 
for the slots. 

Here you can either use an identity map - use keywords as keys
or an object array. 
" )

(defprotocol DisruptorEvent
  (set-slot! [event slot-key value])
  (get-slot  [event slot-key value]))

(extend-protocol DisruptorEvent
  java.util.Map
  (set-slot! [event slot-key value]
             (.put event slot-key value)
             event)
  (get-slot! [event slot-key value]
             (.get event slot-key))
  
   #=(java.lang.Class/forName "[Ljava.lang.Object;")
  (set-slot! [event slot-key value]
             (let [e ^"[Ljava.lang.Object;" event]
               (aset e slot-key value)
               event))
  
  (get-slot  [event slot-key value]
             (let [e ^"[Ljava.lang.Object;" event]
               (aset e slot-key value)))
  )

