module github.com/atomix/go-local

go 1.13

require (
	github.com/atomix/api v0.3.3
	github.com/atomix/api/go v0.3.3
	github.com/atomix/go-framework v0.5.1
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	golang.org/x/net v0.0.0-20190724013045-ca1201d0de80 // indirect
	golang.org/x/sys v0.0.0-20190804053845-51ab0e2deafa // indirect
	golang.org/x/text v0.3.2 // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
)

replace github.com/atomix/api/go => ../atomix-api/go

replace github.com/atomix/go-framework => ../atomix-go-node
