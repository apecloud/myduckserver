module github.com/apecloud/myduckserver

go 1.23.2

require (
	github.com/Shopify/toxiproxy/v2 v2.9.0
	github.com/apache/arrow-go/v18 v18.0.0
	github.com/cockroachdb/apd/v3 v3.2.1
	github.com/cockroachdb/cockroachdb-parser v0.23.2
	github.com/dolthub/doltgresql v0.13.0
	github.com/dolthub/go-mysql-server v0.18.2-0.20241112002228-81b13e8034f2
	github.com/dolthub/vitess v0.0.0-20241111235433-a20a5ab9d7c9
	github.com/go-sql-driver/mysql v1.8.1
	github.com/jackc/pglogrepl v0.0.0-20240307033717-828fbfe908e9
	github.com/jackc/pgx/v5 v5.7.1
	github.com/jmoiron/sqlx v1.4.0
	github.com/lib/pq v1.10.9
	github.com/marcboeker/go-duckdb v1.8.3
	github.com/prometheus/client_golang v1.20.3
	github.com/rs/zerolog v1.33.0
	github.com/shopspring/decimal v1.3.1
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.9.0
	golang.org/x/text v0.19.0
	gopkg.in/src-d/go-errors.v1 v1.0.0
	vitess.io/vitess v0.21.0
)

replace (
	github.com/dolthub/go-mysql-server v0.18.2-0.20241112002228-81b13e8034f2 => github.com/apecloud/go-mysql-server v0.0.0-20241112031328-30cddba3eea7
	github.com/dolthub/vitess v0.0.0-20241111235433-a20a5ab9d7c9 => github.com/apecloud/dolt-vitess v0.0.0-20241112063127-f62e98a9936a
	github.com/marcboeker/go-duckdb v1.8.3 => github.com/apecloud/go-duckdb v0.0.0-20241113073916-47619770e595
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/AdaLogics/go-fuzz-headers v0.0.0-20240806141605-e8a1dd7889d6 // indirect
	github.com/DATA-DOG/go-sqlmock v1.5.2 // indirect
	github.com/bazelbuild/rules_go v0.46.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/biogo/store v0.0.0-20201120204734-aad293a2328f // indirect
	github.com/blevesearch/snowballstem v0.9.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cockroachdb/errors v1.9.0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20211118104740-dabe8e521a4f // indirect
	github.com/cockroachdb/redact v1.1.3 // indirect
	github.com/dave/dst v0.27.2 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dolthub/flatbuffers/v23 v23.3.3-dh.2 // indirect
	github.com/dolthub/go-icu-regex v0.0.0-20240916130659-0118adc6b662 // indirect
	github.com/dolthub/jsonpath v0.0.2-0.20240227200619-19675ab05c71 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/getsentry/sentry-go v0.12.0 // indirect
	github.com/go-kit/kit v0.10.0 // indirect
	github.com/goccy/go-json v0.10.3 // indirect
	github.com/gocraft/dbr/v2 v2.7.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/geo v0.0.0-20210211234256-740aa86cb551 // indirect
	github.com/golang/glog v1.2.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/flatbuffers v24.3.25+incompatible // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/klauspost/cpuid/v2 v2.2.8 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lestrrat-go/strftime v1.0.4 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/petermattis/goid v0.0.0-20180202154549-b0b1615b78e5 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/pierrre/geohash v1.0.0 // indirect
	github.com/pires/go-proxyproto v0.7.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.59.1 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rogpeppe/go-internal v1.12.0 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/sasha-s/go-deadlock v0.3.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tetratelabs/wazero v1.1.0 // indirect
	github.com/twpayne/go-geom v1.4.1 // indirect
	github.com/twpayne/go-kml v1.5.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.opentelemetry.io/otel v1.30.0 // indirect
	go.opentelemetry.io/otel/trace v1.30.0 // indirect
	golang.org/x/crypto v0.27.0 // indirect
	golang.org/x/exp v0.0.0-20240909161429-701f63a606c0 // indirect
	golang.org/x/mod v0.21.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/sys v0.26.0 // indirect
	golang.org/x/tools v0.26.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/genproto v0.0.0-20240903143218-8af14fe29dc1 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240903143218-8af14fe29dc1 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1 // indirect
	google.golang.org/grpc v1.67.1 // indirect
	google.golang.org/protobuf v1.35.1 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
