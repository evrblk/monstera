package codegen

import (
	"fmt"

	. "github.com/dave/jennifer/jen" //lint:ignore ST1001 jen helpers are so much nicer to use with dot-importing
)

func GenerateAdapters(cfg *MonsteraYaml) (string, error) {
	f := NewFilePath(cfg.GoCode.OutputPackage)
	f.HeaderComment(generatedCodeComment)
	f.ImportAlias(mrpcPkg, "mrpc")

	generateMetrics(f)

	for _, core := range cfg.Cores {
		generateAdapter(f, core, cfg)
	}

	generateHelpers(f)

	return fmt.Sprintf("%#v", f), nil
}

func generateMetrics(f *File) {
	f.Var().Defs(
		Id("rpcMethodDuration").Op("=").Qual("github.com/prometheus/client_golang/prometheus", "NewHistogramVec").Call(
			Qual("github.com/prometheus/client_golang/prometheus", "HistogramOpts").Values(Dict{
				Id("Name"):                            Lit("monstera_rpc_method_duration_seconds"),
				Id("Help"):                            Lit("Monstera RPC method duration"),
				Id("NativeHistogramBucketFactor"):     Lit(1.1),
				Id("NativeHistogramMaxBucketNumber"):  Lit(100),
				Id("NativeHistogramMinResetDuration"): Qual("time", "Hour"),
			},
			),
			Op("[]").String().Values(Lit("node"), Lit("core"), Lit("method"), Lit("shard"), Lit("replica")),
		),
		Id("rpcMethodsTotal").Op("=").Qual("github.com/prometheus/client_golang/prometheus", "NewCounterVec").Call(
			Qual("github.com/prometheus/client_golang/prometheus", "CounterOpts").Values(Dict{
				Id("Name"): Lit("monstera_rpc_methods_total"),
				Id("Help"): Lit("Number of Monstera RPC method calls"),
			},
			),
			Op("[]").String().Values(Lit("node"), Lit("core"), Lit("method"), Lit("shard"), Lit("replica")),
		),
	)
	f.Line()

	f.Comment("RegisterMetrics registers the RPC metrics emitted by the generated core")
	f.Comment("adapters with the given registerer. Call once at startup, e.g.")
	f.Comment("RegisterMetrics(prometheus.DefaultRegisterer). It panics if a metric is")
	f.Comment("already registered.")
	f.Func().Id("RegisterMetrics").Params(
		Id("registerer").Qual("github.com/prometheus/client_golang/prometheus", "Registerer"),
	).Block(
		Id("registerer").Dot("MustRegister").Call(Id("rpcMethodDuration")),
		Id("registerer").Dot("MustRegister").Call(Id("rpcMethodsTotal")),
	)
	f.Line()
}

func generateHelpers(f *File) {
	// measureSince func
	f.Func().Id("measureSince").Params(
		Id("o").Qual("github.com/prometheus/client_golang/prometheus", "Observer"),
		Id("t1").Qual("time", "Time"),
	).Block(
		Id("o").Dot("Observe").Call(Qual("time", "Since").Call(Id("t1")).Dot("Seconds").Call()),
	)
	f.Line()

	// checkShardBounds func
	f.Func().Id("checkShardBounds").Params(
		Id("shardKey").Index().Byte(),
		Id("lowerBound").Index().Byte(),
		Id("upperBound").Index().Byte(),
	).Params(
		Error(),
	).Block(
		If(
			Qual("bytes", "Compare").Call(Id("shardKey"), Id("lowerBound")).Op("<").Lit(0).
				Op("||").
				Qual("bytes", "Compare").Call(Id("shardKey"), Id("upperBound")).Op(">").Lit(0),
		).Block(
			Return(Qual("fmt", "Errorf").Call(
				Lit("routing violation: shard key %x is outside shard bounds [%x, %x]"),
				Id("shardKey"),
				Id("lowerBound"),
				Id("upperBound"),
			)),
		),
		Return(Nil()),
	)
	f.Line()
}

func generateAdapter(f *File, core *MonsteraCore, cfg *MonsteraYaml) {
	adapterName := core.Name + "CoreAdapter"
	apiName := core.Name + "CoreApi"
	coreVarName := firstCharToLower(core.Name) + "Core"
	corepb := cfg.GoCode.CoreTypesPackage
	f.Type().Id(adapterName).Struct(
		Id("nodeId").String(),
		Id("shardId").String(),
		Id("replicaId").String(),
		Line(),
		Id("shardLowerBound").Index().Byte(),
		Id("shardUpperBound").Index().Byte(),
		Line(),
		Id(coreVarName).Qual(cfg.GoCode.OutputPackage, apiName),
	)

	// ApplicationCore interface var
	f.Var().Id("_").Qual(monsteraPkg, "ApplicationCore").Op("=").Op("&").Id(adapterName).Values()

	// NewAdapter func
	f.Func().Id(
		"New"+adapterName,
	).Params(
		Id("nodeId").String(),
		Id("shardId").String(),
		Id("replicaId").String(),
		Id("shardLowerBound").Index().Byte(),
		Id("shardUpperBound").Index().Byte(),
		Id(coreVarName).Qual(cfg.GoCode.OutputPackage, apiName),
	).Params(
		Op("*").Id(adapterName),
	).Block(
		Return(Op("&").Id(adapterName).Values(Dict{
			Id("nodeId"):          Id("nodeId"),
			Id("shardId"):         Id("shardId"),
			Id("replicaId"):       Id("replicaId"),
			Id("shardLowerBound"): Id("shardLowerBound"),
			Id("shardUpperBound"): Id("shardUpperBound"),
			Id(coreVarName):       Id(coreVarName),
		})),
	)
	f.Line()

	// Snapshot func
	f.Func().Params(
		Id("a").Op("*").Id(adapterName),
	).Id("Snapshot").Params().Params(
		Qual(monsteraPkg, "ApplicationCoreSnapshot"),
	).Block(
		Return(Id("a").Dot(coreVarName).Dot("Snapshot").Call()),
	)
	f.Line()

	// Restore func
	f.Func().Params(
		Id("a").Op("*").Id(adapterName),
	).Id("Restore").Params(
		Id("readers").Op("...").Qual("io", "ReadCloser"),
	).Params(
		Error(),
	).Block(
		Return(Id("a").Dot(coreVarName).Dot("Restore").Call(Id("readers").Op("..."))),
	)
	f.Line()

	// Close func
	f.Func().Params(
		Id("a").Op("*").Id(adapterName),
	).Id("Close").Params().Block(
		Id("a").Dot(coreVarName).Dot("Close").Call(),
	)
	f.Line()

	// Update func
	f.Func().Params(
		Id("a").Op("*").Id(adapterName),
	).Id("Update").Params(
		Id("rpcReqBytes").Index().Byte(),
	).Params(
		List(
			Op("*").Qual(monsteraPkg, "UpdateResponse"),
			Error(),
		),
	).BlockFunc(func(g *Group) {
		if len(core.UpdateMethods) > 0 {
			g.Id("t1").Op(":=").Qual("time", "Now").Call()
			g.Line()

			g.Id("resp").Op(":=").Op("&").Qual(monsteraPkg, "UpdateResponse").Values()
			g.Id("rpcResp").Op(":=").Op("&").Qual(mrpcPkg, "Response").Values()
			g.Id("rpcReq").Op(":=").Op("&").Qual(mrpcPkg, "Request").Values()
			g.Line()

			g.Err().Op(":=").Id("rpcReq").Dot("UnmarshalVT").Call(
				Id("rpcReqBytes"),
			)
			g.If(
				Err().Op("!=").Nil(),
			).Block(
				Return(Nil(), Err()),
			)
			g.Line()

			g.Switch(
				Id("rpcReq").Dot("MethodNumber"),
			).BlockFunc(func(g *Group) {
				for _, update := range core.UpdateMethods {
					g.Case(
						Lit(update.Number),
					).BlockFunc(func(g *Group) {
						g.Id("rpcMethodsTotal").Dot("WithLabelValues").Call(
							Id("a").Dot("nodeId"),
							Lit(core.Name),
							Lit(update.Name),
							Id("a").Dot("shardId"),
							Id("a").Dot("replicaId"),
						).Dot("Inc").Call()
						g.Defer().Id("measureSince").Call(
							Id("rpcMethodDuration").Dot("WithLabelValues").Call(
								Id("a").Dot("nodeId"),
								Lit(core.Name),
								Lit(update.Name),
								Id("a").Dot("shardId"),
								Id("a").Dot("replicaId"),
							),
							Id("t1"),
						)
						g.Line()

						g.Id("methodReq").Op(":=").Qual(corepb, update.Name+"Request").Op("{}")
						g.Id("err").Op(":=").Id("methodReq").Dot("UnmarshalBinary").Call(
							Id("rpcReq").Dot("Data"),
						)
						g.If(
							Id("err").Op("!=").Nil(),
						).Block(
							Return(Nil(), Err()),
						)
						if update.Sharded {
							g.If(
								Err().Op(":=").Id("checkShardBounds").Call(
									Id("methodReq").Dot("ShardKey").Call(),
									Id("a").Dot("shardLowerBound"),
									Id("a").Dot("shardUpperBound"),
								),
								Err().Op("!=").Nil(),
							).Block(
								Return(Nil(), Err()),
							)
						}
						g.List(Id("methodResp"), Err()).Op(":=").Id("a").Dot(coreVarName).Dot(update.Name).Call(
							Op("&").Id(update.Name + "Request").Values(Dict{
								Id("Payload"): Op("&").Id("methodReq"),
								Id("Now"):     Id("rpcReq").Dot("Now"),
							}),
						)
						g.If(
							Id("err").Op("!=").Nil(),
						).Block(
							Return(Nil(), Err()),
						)
						g.Id("rpcResp").Dot("Error").Op("=").Id("methodResp").Dot("ApplicationError")
						// Payload is nil when the method returns only an
						// ApplicationError; MarshalBinary on a nil payload is
						// not guaranteed to be safe for hand-written types.
						g.If(Id("methodResp").Dot("Payload").Op("!=").Nil()).Block(
							List(Id("methodRespBytes"), Err()).Op(":=").Id("methodResp").Dot("Payload").Dot("MarshalBinary").Call(),
							If(
								Id("err").Op("!=").Nil(),
							).Block(
								Return(Nil(), Err()),
							),
							Id("rpcResp").Dot("Data").Op("=").Id("methodRespBytes"),
						)
						// Id("resp").Dot("Events").Op("=").Id("methodResp").Dot("Events"),
					})
				}
				g.Default().Block(
					Return(Nil(), Qual("fmt", "Errorf").Call(
						Lit("no matching handlers"),
					)),
				)
			})
			g.Line()

			g.List(
				Id("rpcRespBytes"),
				Err(),
			).Op(":=").Id("rpcResp").Dot("MarshalVT").Call()
			g.If(
				Err().Op("!=").Nil(),
			).Block(
				Return(Nil(), Err()),
			)
			g.Id("resp").Dot("Data").Op("=").Id("rpcRespBytes")
			g.Line()

			g.Return(Id("resp"), Nil())
		} else {
			g.Return(Nil(), Qual("fmt", "Errorf").Call(
				Lit("no matching handlers"),
			))
		}
	})
	f.Line()

	// Read func
	f.Func().Params(
		Id("a").Op("*").Id(adapterName),
	).Id("Read").Params(
		Id("rpcReqBytes").Index().Byte(),
	).Params(
		List(
			Op("*").Qual(monsteraPkg, "ReadResponse"),
			Error(),
		),
	).BlockFunc(func(g *Group) {
		if len(core.ReadMethods) > 0 {
			g.Id("t1").Op(":=").Qual("time", "Now").Call()
			g.Line()

			g.Id("resp").Op(":=").Op("&").Qual(monsteraPkg, "ReadResponse").Values()
			g.Id("rpcResp").Op(":=").Op("&").Qual(mrpcPkg, "Response").Values()
			g.Id("rpcReq").Op(":=").Op("&").Qual(mrpcPkg, "Request").Values()
			g.Line()

			g.Err().Op(":=").Id("rpcReq").Dot("UnmarshalVT").Call(
				Id("rpcReqBytes"),
			)
			g.If(
				Err().Op("!=").Nil(),
			).Block(
				Return(Nil(), Err()),
			)
			g.Line()

			g.Switch(
				Id("rpcReq").Dot("MethodNumber"),
			).BlockFunc(func(g *Group) {
				for _, read := range core.ReadMethods {
					g.Case(
						Lit(read.Number),
					).BlockFunc(func(g *Group) {
						g.Id("rpcMethodsTotal").Dot("WithLabelValues").Call(
							Id("a").Dot("nodeId"),
							Lit(core.Name),
							Lit(read.Name),
							Id("a").Dot("shardId"),
							Id("a").Dot("replicaId"),
						).Dot("Inc").Call()
						g.Defer().Id("measureSince").Call(
							Id("rpcMethodDuration").Dot("WithLabelValues").Call(
								Id("a").Dot("nodeId"),
								Lit(core.Name),
								Lit(read.Name),
								Id("a").Dot("shardId"),
								Id("a").Dot("replicaId"),
							),
							Id("t1"),
						)
						g.Line()

						g.Id("methodReq").Op(":=").Qual(corepb, read.Name+"Request").Op("{}")
						g.Id("err").Op(":=").Id("methodReq").Dot("UnmarshalBinary").Call(
							Id("rpcReq").Dot("Data"),
						)
						g.If(
							Id("err").Op("!=").Nil(),
						).Block(
							Return(Nil(), Err()),
						)
						if read.Sharded {
							g.If(
								Err().Op(":=").Id("checkShardBounds").Call(
									Id("methodReq").Dot("ShardKey").Call(),
									Id("a").Dot("shardLowerBound"),
									Id("a").Dot("shardUpperBound"),
								),
								Err().Op("!=").Nil(),
							).Block(
								Return(Nil(), Err()),
							)
						}
						g.List(Id("methodResp"), Err()).Op(":=").Id("a").Dot(coreVarName).Dot(read.Name).Call(
							Op("&").Id(read.Name + "Request").Values(Dict{
								Id("Payload"): Op("&").Id("methodReq"),
								Id("Now"):     Id("rpcReq").Dot("Now"),
							}),
						)
						g.If(
							Id("err").Op("!=").Nil(),
						).Block(
							Return(Nil(), Err()),
						)
						g.Id("rpcResp").Dot("Error").Op("=").Id("methodResp").Dot("ApplicationError")
						// Payload is nil when the method returns only an
						// ApplicationError; MarshalBinary on a nil payload is
						// not guaranteed to be safe for hand-written types.
						g.If(Id("methodResp").Dot("Payload").Op("!=").Nil()).Block(
							List(Id("methodRespBytes"), Err()).Op(":=").Id("methodResp").Dot("Payload").Dot("MarshalBinary").Call(),
							If(
								Id("err").Op("!=").Nil(),
							).Block(
								Return(Nil(), Err()),
							),
							Id("rpcResp").Dot("Data").Op("=").Id("methodRespBytes"),
						)
						// Id("resp").Dot("Events").Op("=").Id("methodResp").Dot("Events"),
					})
				}
				g.Default().Block(
					Return(Nil(), Qual("fmt", "Errorf").Call(
						Lit("no matching handlers"),
					)),
				)
			})
			g.Line()

			g.List(
				Id("rpcRespBytes"),
				Err(),
			).Op(":=").Id("rpcResp").Dot("MarshalVT").Call()
			g.If(
				Err().Op("!=").Nil(),
			).Block(
				Return(Nil(), Err()),
			)
			g.Id("resp").Dot("Data").Op("=").Id("rpcRespBytes")
			g.Line()

			g.Return(Id("resp"), Nil())
		} else {
			g.Return(Nil(), Qual("fmt", "Errorf").Call(
				Lit("no matching handlers"),
			))
		}
	})

	f.Line()
}
