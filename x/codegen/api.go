package codegen

import (
	"fmt"
	"log"

	. "github.com/dave/jennifer/jen"
)

func GenerateCoreApis(cfg *MonsteraYaml) string {
	f := NewFilePath(cfg.GoCode.OutputPackage)
	f.HeaderComment(generatedCodeComment)
	f.ImportAlias(monsterax, "monsterax")

	// Generate request and response type aliases
	for _, core := range cfg.Cores {
		for _, method := range core.ReadMethods {
			requestName := "ReadRequest"
			if !method.Sharded {
				requestName = "ReadUnshardedRequest"
			}
			f.Type().Id(method.Name+"Request").Op("=").Qual(monsterax, requestName).Index(Op("*").Qual(cfg.GoCode.CoreTypesPackage, method.Name+"Request"))
			f.Type().Id(method.Name+"Response").Op("=").Qual(monsterax, "ReadResponse").Index(Op("*").Qual(cfg.GoCode.CoreTypesPackage, method.Name+"Response"))
		}
		for _, method := range core.UpdateMethods {
			requestName := "UpdateRequest"
			if !method.Sharded {
				requestName = "UpdateUnshardedRequest"
			}
			f.Type().Id(method.Name+"Request").Op("=").Qual(monsterax, requestName).Index(Op("*").Qual(cfg.GoCode.CoreTypesPackage, method.Name+"Request"))
			f.Type().Id(method.Name+"Response").Op("=").Qual(monsterax, "UpdateResponse").Index(Op("*").Qual(cfg.GoCode.CoreTypesPackage, method.Name+"Response"))
		}
	}
	f.Line()

	// Generate stub APIs
	for _, stub := range cfg.Stubs {
		cores := make([]*MonsteraCore, len(stub.Cores))
		for i, coreName := range stub.Cores {
			found := false
			for _, core := range cfg.Cores {
				if core.Name == coreName {
					cores[i] = core
					found = true
					break
				}
			}

			if !found {
				log.Fatalf("core %s not found", coreName)
			}
		}

		generateStubApi(f, stub, cores, cfg)
	}

	// Generate core APIs
	for _, core := range cfg.Cores {
		generateCoreApi(f, core)
	}

	return fmt.Sprintf("%#v", f)
}

func generateCoreApi(f *File, core *MonsteraCore) {
	apiName := core.Name + "CoreApi"
	f.Type().Id(apiName).InterfaceFunc(func(g *Group) {
		g.Id("Snapshot").Params().Qual("github.com/evrblk/monstera", "ApplicationCoreSnapshot")
		g.Id("Restore").Params(Id("reader").Qual("io", "ReadCloser")).Error()
		g.Id("Close").Params()

		methods := make([]string, 0, len(core.ReadMethods)+len(core.UpdateMethods))
		for _, method := range core.ReadMethods {
			methods = append(methods, method.Name)
		}
		for _, method := range core.UpdateMethods {
			methods = append(methods, method.Name)
		}

		for _, method := range methods {
			g.Id(method).Params(
				Id("req").Op("*").Id(method + "Request"),
			).Params(
				List(
					Op("*").Id(method+"Response"),
					Error(),
				),
			)
		}
	})
	f.Line()
}

func generateStubApi(f *File, stub *MonsteraStub, cores []*MonsteraCore, cfg *MonsteraYaml) {
	apiName := stub.Name + "ClientApi"
	f.Type().Id(apiName).InterfaceFunc(func(g *Group) {
		for _, core := range cores {
			for _, read := range core.ReadMethods {
				generateStubApiMethod(g, read.Name, read.Sharded, cfg)
			}
			for _, update := range core.UpdateMethods {
				generateStubApiMethod(g, update.Name, update.Sharded, cfg)
			}
			g.Line()
		}
		g.Line()
		g.Id("ListShards").Params(Id("applicationName").String()).Params(List(Index().String(), Error()))
	})
}

func generateStubApiMethod(g *Group, method string, sharded bool, cfg *MonsteraYaml) {
	if sharded {
		g.Id(method).Params(
			Id("ctx").Qual("context", "Context"),
			Id("req").Op("*").Qual(cfg.GoCode.CoreTypesPackage, method+"Request"),
		).Params(
			List(
				Op("*").Qual(cfg.GoCode.CoreTypesPackage, method+"Response"),
				Error(),
			),
		)
	} else {
		g.Id(method).Params(
			Id("ctx").Qual("context", "Context"),
			Id("req").Op("*").Qual(cfg.GoCode.CoreTypesPackage, method+"Request"),
			Id("shardId").String(),
		).Params(
			List(
				Op("*").Qual(cfg.GoCode.CoreTypesPackage, method+"Response"),
				Error(),
			),
		)
	}
}
