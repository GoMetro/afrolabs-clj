# version-info

## NAME

`:afrolabs.components.version-info/version-info` — git version of the
running service.

## SYNOPSIS

```clojure
(require '[afrolabs.components.version-info :as -version-info])

;; integrant key
:afrolabs.components.version-info/version-info

;; protocol
(-version-info/version-info component) ;; => {:git-sha "..." :git-ref "..."}

;; functions
(-version-info/read-version-info "version.edn")
(-version-info/make-version-info "version.edn")
```

## DESCRIPTION

Reads the version info resource once at initialisation. The component value
is the map `{:git-sha :git-ref}`, which also implements `IVersionInfo`.
Reference the component from components that report the version.

The resource is an EDN file on the classpath, usually written by the build:

```clojure
{:git-ref "refs/heads/main"
 :git-sha "f7ba5fa0d8bfd1c8a078cae583f8b4cda39b59c3"}
```

When the resource cannot be read, the version comes from the git checkout in
the working directory, `:git-sha` suffixed with `:DIRTY` when there are
uncommitted changes. Without a checkout, the values are `"GIT-REF-FALLBACK"`
and `"GIT-SHA-FALLBACK"`.

## CONFIGURATION

`:version-info-resource`
: Required. Classpath resource path of the EDN file.

## PROTOCOL

`IVersionInfo`
: `(version-info [_])` returns `{:git-sha :git-ref}`. Extensible via
  metadata.

## USED BY

`:afrolabs.components.http/git-version-middleware`
: `:version-info` or `:version-info-resource`, exactly one.

`:afrolabs.components.http/health-endpoint`
: `:version-info` or `:version-info-resource`, at most one.

`:afrolabs.components.sentry/client`
: `:version-info`; `:git-sha` is the Sentry release.

## EXAMPLES

```clojure
:afrolabs.components.version-info/version-info
{:version-info-resource "version.edn"}

:afrolabs.components.http/git-version-middleware
{:version-info #ig/ref :afrolabs.components.version-info/version-info}

:afrolabs.components.http/health-endpoint
{:health-component #ig/ref :afrolabs.components.health/component
 :endpoint         "/health"
 :version-info     #ig/ref :afrolabs.components.version-info/version-info}

:afrolabs.components.sentry/client
{:dsn          #option SENTRY_DSN
 :version-info #ig/ref :afrolabs.components.version-info/version-info}
```

## SEE ALSO

[sentry](sentry.md)
