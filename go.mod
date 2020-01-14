module github.com/atomix/atomix-go-local

go 1.12

require (
	github.com/atomix/atomix-api v0.0.0-20200114202737-fac5129dc110
	github.com/atomix/atomix-go-node v0.0.0-20200114210733-967537ba5e31
	github.com/golang/protobuf v1.3.2
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0
	google.golang.org/grpc v1.23.1
)

replace github.com/atomix/atomix-go-node => ../atomix-go-node
