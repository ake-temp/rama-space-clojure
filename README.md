# RamaSpace example in Clojure

A pure clojure + rama implementation of the RamaSpace example from the [Tying it all together](https://redplanetlabs.com/docs/~/tutorial6.html) example provided by Red Planet Labs for [rama](https://redplanetlabs.com/learn-rama).

## Running

I've used [babashka](https://github.com/babashka/babashka) to write the scripts for this repo.

To start a repl you can just run:
```bash
$ bb repl
```

To run the tests:
```bash
$ bb test
```

## Notes

You can find tests in `test/rama_space/module_test.clj`.

I was able to use clojure maps and records without any extra configuration, even `.subSource` worked just fine.

I found it helpful to introduce a couple of helper functions which can be found in `src/rama/*`.
Most of these are just simple wrappers dealing with java varargs.

In `src/rama/core.clj` are some more interesting ones.

`fn->rama-function` is a helper macro to wrap clojure functions to a form that rama can take. This doesn't always work though, for example I had to create a class `KeywordizeKeys` for `Path.view`, presumably for similar reasons that `Depot/hashBy` requires a class.

`extract-map-fields` is basically a copy of `extractJavaFields` from [rama-helpers](https://github.com/redplanetlabs/rama-helpers).

In `src/rama_space/client.clj` I made a client map which was convenient to use, but presuming `.clusterPState` and friends are cheap then it's probably not needed.

I found I was forced to create a type for `Depot/hashBy` as it takes either something that implements `com.rpl.rama.impl.NativeRamaFunction1` (which we don't have access to) or a Class.
It would be nice if we could just supply a function, but I assume this is something that needs to be transmitted on the wire.
