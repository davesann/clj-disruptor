# clj-disruptor

clojure wrapper for the LMax Disruptor.

references :

* http://martinfowler.com/articles/lmax.html
* http://www.infoq.com/presentations/LMAX

## Usage

* clojars [clj-disruptor "0.0.1"]

```clojure
(require '[examples.disruptor.eg1 :as eg1] :reload)

;; 100 events published
(eg1/go 100)

;; takes ~ 2s on my machine
(eg1/go 1000000)

;; takes ~ 15s on my machine
(eg1/go 10000000)
```

See docs


## License

Copyright (C) 2012 Dave Sann

Distributed under the Eclipse Public License, the same as Clojure.
