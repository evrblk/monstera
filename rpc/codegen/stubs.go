package codegen

import (
	"fmt"

	. "github.com/dave/jennifer/jen" //lint:ignore ST1001 jen helpers are so much nicer to use with dot-importing
)

func GenerateStubs(cfg *MonsteraYaml) (string, error) {
	f := NewFilePath(cfg.GoCode.OutputPackage)
	f.HeaderComment(generatedCodeComment)
	f.ImportAlias(mrpcPkg, "mrpc")

	if len(cfg.Stubs) > 0 {
		generateNilifyIfEmpty(f)
	}

	// Nonclustered core adapter types are keyed by core name, so when several
	// stubs share a core the type is emitted only once.
	emittedAdapters := make(map[string]bool)

	for _, stub := range cfg.Stubs {
		cores, err := resolveStubCores(cfg, stub)
		if err != nil {
			return "", err
		}

		generateMonsteraStub(f, stub, cores, cfg)
		generateNonclusteredStub(f, stub, cores, cfg, emittedAdapters)
	}

	return fmt.Sprintf("%#v", f), nil
}

// generateShardKeyLengthCheck emits the same shard-key contract check the
// clustered path enforces (cluster.Config.FindShardByShardKey), so an invalid
// key fails identically in nonclustered (dev) and clustered mode instead of
// only on deploy.
func generateShardKeyLengthCheck(g *Group) {
	g.If(Len(Id("shardKey")).Op("==").Lit(0).Op("||").Len(Id("shardKey")).Op(">").Lit(4)).Block(
		Return(Nil(), Qual("fmt", "Errorf").Call(
			Lit("invalid shard key length %d: must be between 1 and 4 bytes"),
			Len(Id("shardKey")),
		)),
	)
}

// generateNilifyIfEmpty emits the shared helper that converts an empty/OK
// rpc.Error into a nil error. It is package-level and must be emitted exactly
// once per file, regardless of how many stubs share it.
func generateNilifyIfEmpty(f *File) {
	f.Func().Id("nilifyIfEmpty").Params(
		Err().Op("*").Qual(mrpcPkg, "Error"),
	).Params(
		Error(),
	).Block(
		If(
			Err().Op("==").Nil().Op("||").Id("err.Code").Op("==").Qual(mrpcPkg, "ErrorCode_INVALID").Op("||").Id("err.Code").Op("==").Qual(mrpcPkg, "ErrorCode_OK"),
		).Block(
			Return(Nil()),
		).Else().Block(
			Return(Err()),
		),
	)
	f.Line()
}

func generateMonsteraStub(f *File, stub *MonsteraStub, cores []*MonsteraCore, cfg *MonsteraYaml) {
	stubType := stub.Name + "MonsteraStub"

	// type <Stub>
	f.Type().Id(stubType).StructFunc(func(g *Group) {
		g.Id("monsteraClient").Op("*").Qual(monsteraPkg, "Client")
	})
	apiName := stub.Name + "ClientApi"
	f.Var().Id("_").Qual(cfg.GoCode.OutputPackage, apiName).Op("=").Op("&").Id(stubType).Values()
	f.Line()

	stubReceiver := Id("s").Op("*").Id(stubType)

	for _, core := range cores {
		for _, read := range core.ReadMethods {
			f.Func().Params(stubReceiver).Id(read.Name).ParamsFunc(func(g *Group) {
				g.Id("ctx").Qual("context", "Context")
				g.Id("methodReq").Op("*").Qual(cfg.GoCode.CoreTypesPackage, read.Name+"Request")
				if !read.Sharded {
					g.Id("shardId").String()
				}
			}).Params(
				List(
					Op("*").Qual(cfg.GoCode.CoreTypesPackage, read.Name+"Response"),
					Error(),
				),
			).BlockFunc(func(g *Group) {
				g.List(Id("methodReqBytes"), Err()).Op(":=").Id("methodReq").Dot("MarshalBinary").Call()
				g.If(
					Err().Op("!=").Nil(),
				).Block(
					Return(Id("nil"), Qual("fmt", "Errorf").Call(
						Lit("failed to marshal request: %w"),
						Err(),
					)),
				)
				g.Line()

				g.Id("rpcReq").Op(":=").Op("&").Qual(mrpcPkg, "Request").Values(Dict{
					Id("MethodNumber"): Lit(read.Number),
					Id("Data"):         Id("methodReqBytes"),
					Id("Now"):          Qual("time", "Now").Call().Dot("UnixNano").Call(),
				})
				g.List(Id("rpcReqBytes"), Err()).Op(":=").Id("rpcReq").Dot("MarshalVT").Call()
				g.If(
					Err().Op("!=").Nil(),
				).Block(
					Return(Id("nil"), Qual("fmt", "Errorf").Call(
						Lit("failed to marshal request: %w"),
						Err(),
					)),
				)
				g.Line()

				if read.Sharded {
					g.List(Id("rpcRespBytes"), Err()).Op(":=").Id("s.monsteraClient.Read").Call(
						Id("ctx"),
						Lit(core.Name),
						Id("methodReq").Dot("ShardKey").Call(),
						Lit(read.AllowReadFromFollowers),
						Id("rpcReqBytes"),
					)
				} else {
					g.Line()

					g.List(Id("rpcRespBytes"), Err()).Op(":=").Id("s.monsteraClient.ReadShard").Call(
						Id("ctx"),
						Lit(core.Name),
						Id("shardId"),
						Lit(read.AllowReadFromFollowers),
						Id("rpcReqBytes"),
					)
				}

				g.If(
					Err().Op("!=").Nil(),
				).Block(
					Return(Id("nil"), Err()),
				)
				g.Line()

				g.Id("rpcResp").Op(":=").Op("&").Qual(mrpcPkg, "Response").Values()
				g.Err().Op("=").Id("rpcResp").Dot("UnmarshalVT").Call(Id("rpcRespBytes"))
				g.If(
					Err().Op("!=").Nil(),
				).Block(
					Return(Id("nil"), Qual("fmt", "Errorf").Call(
						Lit("failed to unmarshal response: %w"),
						Err(),
					)),
				)
				g.Id("methodResp").Op(":=").Op("&").Qual(cfg.GoCode.CoreTypesPackage, read.Name+"Response").Values()
				g.Err().Op("=").Id("methodResp").Dot("UnmarshalBinary").Call(Id("rpcResp").Dot("Data"))
				g.If(
					Err().Op("!=").Nil(),
				).Block(
					Return(Id("nil"), Qual("fmt", "Errorf").Call(
						Lit("failed to unmarshal response: %w"),
						Err(),
					)),
				)
				g.Line()

				g.Return(Id("methodResp"), Id("nilifyIfEmpty").Call(Id("rpcResp").Dot("Error")))
			})
			f.Line()
		}

		for _, update := range core.UpdateMethods {
			f.Func().Params(stubReceiver).Id(update.Name).ParamsFunc(func(g *Group) {
				g.Id("ctx").Qual("context", "Context")
				g.Id("methodReq").Op("*").Qual(cfg.GoCode.CoreTypesPackage, update.Name+"Request")
				if !update.Sharded {
					g.Id("shardId").String()
				}
			}).Params(
				List(
					Op("*").Qual(cfg.GoCode.CoreTypesPackage, update.Name+"Response"),
					Error(),
				),
			).BlockFunc(func(g *Group) {
				g.List(Id("methodReqBytes"), Err()).Op(":=").Id("methodReq").Dot("MarshalBinary").Call()
				g.If(
					Err().Op("!=").Nil(),
				).Block(
					Return(Id("nil"), Qual("fmt", "Errorf").Call(
						Lit("failed to marshal request: %w"),
						Err(),
					)),
				)
				g.Line()

				g.Id("rpcReq").Op(":=").Op("&").Qual(mrpcPkg, "Request").Values(Dict{
					Id("MethodNumber"): Lit(update.Number),
					Id("Data"):         Id("methodReqBytes"),
					Id("Now"):          Qual("time", "Now").Call().Dot("UnixNano").Call(),
				})
				g.List(Id("rpcReqBytes"), Err()).Op(":=").Id("rpcReq").Dot("MarshalVT").Call()
				g.If(
					Err().Op("!=").Nil(),
				).Block(
					Return(Id("nil"), Qual("fmt", "Errorf").Call(
						Lit("failed to marshal request: %w"),
						Err(),
					)),
				)
				g.Line()

				if update.Sharded {
					g.List(Id("rpcRespBytes"), Err()).Op(":=").Id("s.monsteraClient.Update").Call(
						Id("ctx"),
						Lit(core.Name),
						Id("methodReq").Dot("ShardKey").Call(),
						Id("rpcReqBytes"),
					)
				} else {
					g.Line()

					g.List(Id("rpcRespBytes"), Err()).Op(":=").Id("s.monsteraClient.UpdateShard").Call(
						Id("ctx"),
						Lit(core.Name),
						Id("shardId"),
						Id("rpcReqBytes"),
					)
				}

				g.If(
					Err().Op("!=").Nil(),
				).Block(
					Return(Id("nil"), Err()),
				)
				g.Line()

				g.Id("rpcResp").Op(":=").Op("&").Qual(mrpcPkg, "Response").Values()
				g.Err().Op("=").Id("rpcResp").Dot("UnmarshalVT").Call(Id("rpcRespBytes"))
				g.If(
					Err().Op("!=").Nil(),
				).Block(
					Return(Id("nil"), Qual("fmt", "Errorf").Call(
						Lit("failed to unmarshal response: %w"),
						Err(),
					)),
				)
				g.Id("methodResp").Op(":=").Op("&").Qual(cfg.GoCode.CoreTypesPackage, update.Name+"Response").Values()
				g.Err().Op("=").Id("methodResp").Dot("UnmarshalBinary").Call(Id("rpcResp").Dot("Data"))
				g.If(
					Err().Op("!=").Nil(),
				).Block(
					Return(Id("nil"), Qual("fmt", "Errorf").Call(
						Lit("failed to unmarshal response: %w"),
						Err(),
					)),
				)
				g.Line()

				g.Return(Id("methodResp"), Id("nilifyIfEmpty").Call(Id("rpcResp").Dot("Error")))
			})
			f.Line()
		}
	}

	// func ListShards()
	f.Func().Params(stubReceiver).Id("ListShards").Params(
		Id("applicationName").String(),
	).Params(
		List(Index().String(), Error()),
	).BlockFunc(func(g *Group) {
		g.List(Id("shards"), Err()).Op(":=").Id("s").Dot("monsteraClient").Dot("ListShards").Call(Id("applicationName"))
		g.If(Err().Op("!=").Nil()).Block(
			Return(Id("nil"), Err()),
		)
		g.Id("shardIds").Op(":=").Make(Index().String(), Len(Id("shards")))
		g.For(Id("i").Op(":=").Range().Id("shards")).Block(
			Id("shardIds").Index(Id("i")).Op("=").Id("shards").Index(Id("i")).Dot("Id"),
		)
		g.Return(Id("shardIds"), Nil())
	})
	f.Line()

	// func New<Stub>
	f.Func().Id("New" + stubType).Params(
		Id("monsteraClient").Op("*").Qual(monsteraPkg, "Client"),
	).Params(
		Op("*").Id(stubType),
	).Block(
		Return(
			Op("&").Id(stubType).Values(
				Id("monsteraClient").Op(":").Id("monsteraClient"),
			)),
	)
	f.Line()
}

func generateNonclusteredStub(f *File, stub *MonsteraStub, cores []*MonsteraCore, cfg *MonsteraYaml, emittedAdapters map[string]bool) {
	adapterTypeName := func(coreName string) string {
		return firstCharToLower(coreName) + "CoreNonclusteredAdapter"
	}

	// Core adapters for internal shards. Keyed by core name, shared between
	// stubs: emit each one only once per file.
	for _, core := range cores {
		if emittedAdapters[core.Name] {
			continue
		}
		emittedAdapters[core.Name] = true
		f.Type().Id(adapterTypeName(core.Name)).StructFunc(func(g *Group) {
			g.Id("core").Qual(cfg.GoCode.OutputPackage, core.Name+"CoreApi")
			g.Id("mu").Qual("sync", "RWMutex")
			g.Id("id").String()
			g.Id("lowerBound").Index().Byte()
			g.Id("upperBound").Index().Byte()
		})
		f.Line()
	}

	// type <Stub>NonclusteredApplicationCoresFactory
	applicationCoresFactoryType := stub.Name + "NonclusteredApplicationCoresFactory"
	f.Type().Id(applicationCoresFactoryType).StructFunc(func(g *Group) {
		for _, core := range cores {
			g.Id(core.Name+"CoreFactoryFunc").Func().Params(Id("shardId").String(), Id("lowerBound").Index().Byte(), Id("upperBound").Index().Byte()).Qual(cfg.GoCode.OutputPackage, core.Name+"CoreApi")
		}
	})

	stubType := stub.Name + "NonclusteredStub"

	// type <Stub>
	f.Type().Id(stubType).StructFunc(func(g *Group) {
		for _, core := range cores {
			g.Id(firstCharToLower(core.Name) + "Cores").Index().Op("*").Id(adapterTypeName(core.Name))
		}
	})
	apiName := stub.Name + "ClientApi"
	f.Var().Id("_").Qual(cfg.GoCode.OutputPackage, apiName).Op("=").Op("&").Id(stubType).Values()
	f.Line()

	stubReceiver := Id("s").Op("*").Qual(cfg.GoCode.OutputPackage, stubType)

	for _, core := range cores {
		for _, read := range core.ReadMethods {
			f.Func().Params(stubReceiver).Id(read.Name).ParamsFunc(func(g *Group) {
				g.Id("ctx").Qual("context", "Context")
				g.Id("req").Op("*").Qual(cfg.GoCode.CoreTypesPackage, read.Name+"Request")
				if !read.Sharded {
					g.Id("shardId").String()
				}
			}).Params(
				List(
					Op("*").Qual(cfg.GoCode.CoreTypesPackage, read.Name+"Response"),
					Error(),
				),
			).BlockFunc(func(g *Group) {
				if read.Sharded {
					g.Id("shardKey").Op(":=").Id("req").Dot("ShardKey").Call()
					generateShardKeyLengthCheck(g)
					g.For(List(Id("_"), Id("adapter")).Op(":=").Range().Id("s").Dot(firstCharToLower(core.Name) + "Cores")).Block(
						If(Qual("bytes", "Compare").Call(Id("shardKey"), Id("adapter").Dot("upperBound")).Op("<=").Lit(0).Op("&&").
							Qual("bytes", "Compare").Call(Id("shardKey"), Id("adapter").Dot("lowerBound")).Op(">=").Lit(0)).Block(
							Id("adapter").Dot("mu").Dot("RLock").Call(),
							Defer().Id("adapter").Dot("mu").Dot("RUnlock").Call(),
							Line(),
							List(Id("resp"), Err()).Op(":=").Id("adapter").Dot("core").Dot(read.Name).Call(
								Op("&").Qual(mrpcPkg, "ReadRequest").Index(Op("*").Qual(cfg.GoCode.CoreTypesPackage, read.Name+"Request")).Values(Dict{
									Id("Payload"): Id("req"),
									Id("Now"):     Qual("time", "Now").Call().Dot("UnixNano").Call(),
								}),
							),
							If(Err().Op("!=").Nil()).Block(
								Return(Nil(), Err()),
							),
							Err().Op("=").Id("nilifyIfEmpty").Call(Id("resp").Dot("ApplicationError")),
							If(Err().Op("!=").Nil()).Block(
								Return(Nil(), Err()),
							),
							Return(Id("resp").Dot("Payload"), Nil()),
						),
					)
					g.Line()

					g.Return(List(Nil(), Qual("fmt", "Errorf").Call(Lit("no shard found for shardKey: %x"), Id("shardKey"))))
				} else {
					g.For(List(Id("_"), Id("adapter")).Op(":=").Range().Id("s").Dot(firstCharToLower(core.Name) + "Cores")).Block(
						If(Id("adapter").Dot("id").Op("==").Id("shardId")).Block(
							Id("adapter").Dot("mu").Dot("RLock").Call(),
							Defer().Id("adapter").Dot("mu").Dot("RUnlock").Call(),
							Line(),
							List(Id("resp"), Err()).Op(":=").Id("adapter").Dot("core").Dot(read.Name).Call(
								Op("&").Qual(mrpcPkg, "ReadUnshardedRequest").Index(Op("*").Qual(cfg.GoCode.CoreTypesPackage, read.Name+"Request")).Values(Dict{
									Id("Payload"): Id("req"),
									Id("Now"):     Qual("time", "Now").Call().Dot("UnixNano").Call(),
								}),
							),
							If(Err().Op("!=").Nil()).Block(
								Return(Nil(), Err()),
							),
							Err().Op("=").Id("nilifyIfEmpty").Call(Id("resp").Dot("ApplicationError")),
							If(Err().Op("!=").Nil()).Block(
								Return(Nil(), Err()),
							),
							Return(Id("resp").Dot("Payload"), Nil()),
						),
					)
					g.Line()

					g.Return(List(Nil(), Qual("fmt", "Errorf").Call(Lit("no shard found for shardId: %s"), Id("shardId"))))
				}
			})
			f.Line()
		}

		for _, update := range core.UpdateMethods {
			f.Func().Params(stubReceiver).Id(update.Name).ParamsFunc(func(g *Group) {
				g.Id("ctx").Qual("context", "Context")
				g.Id("req").Op("*").Qual(cfg.GoCode.CoreTypesPackage, update.Name+"Request")
				if !update.Sharded {
					g.Id("shardId").String()
				}
			}).Params(
				List(
					Op("*").Qual(cfg.GoCode.CoreTypesPackage, update.Name+"Response"),
					Error(),
				),
			).BlockFunc(func(g *Group) {
				if update.Sharded {
					g.Id("shardKey").Op(":=").Id("req").Dot("ShardKey").Call()
					generateShardKeyLengthCheck(g)
					g.For(List(Id("_"), Id("adapter")).Op(":=").Range().Id("s").Dot(firstCharToLower(core.Name) + "Cores")).Block(
						If(Qual("bytes", "Compare").Call(Id("shardKey"), Id("adapter").Dot("upperBound")).Op("<=").Lit(0).Op("&&").
							Qual("bytes", "Compare").Call(Id("shardKey"), Id("adapter").Dot("lowerBound")).Op(">=").Lit(0)).Block(
							Id("adapter").Dot("mu").Dot("Lock").Call(),
							Defer().Id("adapter").Dot("mu").Dot("Unlock").Call(),
							Line(),
							List(Id("resp"), Err()).Op(":=").Id("adapter").Dot("core").Dot(update.Name).Call(
								Op("&").Qual(mrpcPkg, "UpdateRequest").Index(Op("*").Qual(cfg.GoCode.CoreTypesPackage, update.Name+"Request")).Values(Dict{
									Id("Payload"): Id("req"),
									Id("Now"):     Qual("time", "Now").Call().Dot("UnixNano").Call(),
								}),
							),
							If(Err().Op("!=").Nil()).Block(
								Return(Nil(), Err()),
							),
							Err().Op("=").Id("nilifyIfEmpty").Call(Id("resp").Dot("ApplicationError")),
							If(Err().Op("!=").Nil()).Block(
								Return(Nil(), Err()),
							),
							Return(Id("resp").Dot("Payload"), Nil()),
						),
					)
					g.Line()

					g.Return(List(Nil(), Qual("fmt", "Errorf").Call(Lit("no shard found for shardKey: %x"), Id("shardKey"))))
				} else {
					g.For(List(Id("_"), Id("adapter")).Op(":=").Range().Id("s").Dot(firstCharToLower(core.Name) + "Cores")).Block(
						If(Id("adapter").Dot("id").Op("==").Id("shardId")).Block(
							Id("adapter").Dot("mu").Dot("Lock").Call(),
							Defer().Id("adapter").Dot("mu").Dot("Unlock").Call(),
							Line(),
							List(Id("resp"), Err()).Op(":=").Id("adapter").Dot("core").Dot(update.Name).Call(
								Op("&").Qual(mrpcPkg, "UpdateUnshardedRequest").Index(Op("*").Qual(cfg.GoCode.CoreTypesPackage, update.Name+"Request")).Values(Dict{
									Id("Payload"): Id("req"),
									Id("Now"):     Qual("time", "Now").Call().Dot("UnixNano").Call(),
								}),
							),
							If(Err().Op("!=").Nil()).Block(
								Return(Nil(), Err()),
							),
							Err().Op("=").Id("nilifyIfEmpty").Call(Id("resp").Dot("ApplicationError")),
							If(Err().Op("!=").Nil()).Block(
								Return(Nil(), Err()),
							),
							Return(Id("resp").Dot("Payload"), Nil()),
						),
					)
					g.Line()

					g.Return(List(Nil(), Qual("fmt", "Errorf").Call(Lit("no shard found for shardId: %s"), Id("shardId"))))
				}

			})
			f.Line()
		}
	}

	// func ListShards()
	f.Func().Params(stubReceiver).Id("ListShards").Params(
		Id("applicationName").String(),
	).Params(
		List(Index().String(), Error()),
	).BlockFunc(func(g *Group) {
		g.Switch(Id("applicationName")).BlockFunc(func(g *Group) {
			for _, core := range cores {
				adapters := firstCharToLower(core.Name) + "Cores"
				g.Case(Lit(core.Name)).Block(
					Id("shardIds").Op(":=").Make(Index().String(), Len(Id("s").Dot(adapters))),
					For(Id("i").Op(":=").Range().Id("s").Dot(adapters)).Block(
						Id("shardIds").Index(Id("i")).Op("=").Id("s").Dot(adapters).Index(Id("i")).Dot("id"),
					),
					Return(List(Id("shardIds"), Nil())),
				)
			}
			g.Default().Block(
				Return(List(Nil(), Qual("fmt", "Errorf").Call(Lit("application not found: %s"), Id("applicationName")))),
			)
		})
	})
	f.Line()

	// func New<Stub>()
	f.Func().Id("New"+stubType).Params(
		Id("shardsPerApp").Int(),
		Id("coresFactory").Op("*").Id(applicationCoresFactoryType),
	).Params(
		Op("*").Id(stubType),
	).BlockFunc(func(g *Group) {
		// Shards must evenly partition the 4-byte keyspace: a shardsPerApp
		// that does not divide 2^32 would leave the top of the keyspace
		// unroutable, and there is no cluster config Validate() in this mode
		// to catch it.
		g.If(
			Id("shardsPerApp").Op("<").Lit(1).
				Op("||").Int64().Call(Id("shardsPerApp")).Op(">").Int64().Call(Qual(clusterPkg, "KeyspacePerApplication")).
				Op("||").Id("shardsPerApp").Op("&").Parens(Id("shardsPerApp").Op("-").Lit(1)).Op("!=").Lit(0),
		).Block(
			Panic(Qual("fmt", "Sprintf").Call(
				Lit("shardsPerApp must be a power of 2 between 1 and 2^32, got %d"),
				Id("shardsPerApp"),
			)),
		)
		g.Line()

		for _, core := range cores {
			g.Id(firstCharToLower(core.Name)+"Cores").Op(":=").Make(Index().Op("*").Id(adapterTypeName(core.Name)), Id("shardsPerApp"))
		}
		g.Line()

		g.Id("shardSize").Op(":=").Int64().Call(Qual(clusterPkg, "KeyspacePerApplication")).Op("/").Int64().Call(Id("shardsPerApp"))

		g.For(Id("i").Op(":=").Lit(0), Id("i").Op("<").Id("shardsPerApp"), Id("i").Op("++")).BlockFunc(func(g *Group) {
			g.Id("lower").Op(":=").Uint32().Call(Int64().Call(Id("i")).Op("*").Id("shardSize"))
			g.Id("upper").Op(":=").Uint32().Call(Int64().Call(Id("i").Op("+").Lit(1)).Op("*").Id("shardSize").Op("-").Lit(1))
			g.Id("lowerBound").Op(":=").Make(Index().Byte(), Lit(4))
			g.Id("upperBound").Op(":=").Make(Index().Byte(), Lit(4))
			g.Qual("encoding/binary", "BigEndian").Dot("PutUint32").Call(Id("lowerBound"), Id("lower"))
			g.Qual("encoding/binary", "BigEndian").Dot("PutUint32").Call(Id("upperBound"), Id("upper"))
			g.Line()

			g.List(Id("sl"), Id("su")).Op(":=").Qual(clusterPkg, "ShortenBounds").Call(Id("lowerBound"), Id("upperBound"))
			g.Line()

			for _, core := range cores {
				shardIdVarName := firstCharToLower(core.Name) + "ShardId"
				g.Id(shardIdVarName).Op(":=").Qual("fmt", "Sprintf").Call(Lit("%s_%x_%x"), Lit(core.Name), Id("sl"), Id("su"))
				g.Id(firstCharToLower(core.Name)+"Cores").Index(Id("i")).Op("=").Op("&").Id(adapterTypeName(core.Name)).Values(
					Id("core").Op(":").Id("coresFactory").Dot(core.Name+"CoreFactoryFunc").Call(Id(shardIdVarName), Id("lowerBound"), Id("upperBound")),
					Id("id").Op(":").Id(shardIdVarName),
					Id("lowerBound").Op(":").Id("lowerBound"),
					Id("upperBound").Op(":").Id("upperBound"),
				)
				g.Line()
			}
		})

		g.Return(
			Op("&").Id(stubType).ValuesFunc(func(g *Group) {
				for _, core := range cores {
					g.Id(firstCharToLower(core.Name) + "Cores").Op(":").Id(firstCharToLower(core.Name) + "Cores")
				}
			}))
	})
}
