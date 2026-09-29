all: prep test

test: prep
	( clojure -X:test )

.deps.prepped: deps.edn
	( clojure -X:deps prep )
	touch .deps.prepped

prep: .deps.prepped
