package codegen

import (
	"fmt"

	. "github.com/dave/jennifer/jen" //lint:ignore ST1001 jen helpers are so much nicer to use with dot-importing
)

func GenerateAdapters(cfg *MonsteraYaml) string {
	f := NewFilePath(cfg.GoCode.OutputPackage)
	f.HeaderComment(generatedCodeComment)
	f.ImportAlias(mrpcPkg, "mrpc")

	// Core metrics
	f.Var().Defs(
		Id("monsteraCoreMethodDuration").Op("=").Qual("github.com/prometheus/client_golang/prometheus", "NewHistogramVec").Call(
			Qual("github.com/prometheus/client_golang/prometheus", "HistogramOpts").Values(Dict{
				Id("Name"):                            Lit("monstera_core_method_duration_seconds"),
				Id("Help"):                            Lit("Monstera core method duration"),
				Id("NativeHistogramBucketFactor"):     Lit(1.1),
				Id("NativeHistogramMaxBucketNumber"):  Lit(100),
				Id("NativeHistogramMinResetDuration"): Qual("time", "Hour"),
			},
			),
			Op("[]").String().Values(Lit("core"), Lit("method"), Lit("shard"), Lit("replica")),
		),
		Id("monsteraCoreMethodCount").Op("=").Qual("github.com/prometheus/client_golang/prometheus", "NewCounterVec").Call(
			Qual("github.com/prometheus/client_golang/prometheus", "CounterOpts").Values(Dict{
				Id("Name"): Lit("monstera_core_method_count"),
				Id("Help"): Lit("Monstera core method count"),
			},
			),
			Op("[]").String().Values(Lit("core"), Lit("method"), Lit("shard"), Lit("replica")),
		),
	)
	f.Line()

	for _, core := range cfg.Cores {
		generateAdapter(f, core, cfg)
	}

	generateHelpers(f)

	return fmt.Sprintf("%#v", f)
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
}

func generateAdapter(f *File, core *MonsteraCore, cfg *MonsteraYaml) {
	adapterName := core.Name + "CoreAdapter"
	apiName := core.Name + "CoreApi"
	coreVarName := firstCharToLower(core.Name) + "Core"
	corepb := cfg.GoCode.CoreTypesPackage
	f.Type().Id(adapterName).Struct(
		Id("shardId").String(),
		Id("replicaId").String(),
		Line(),
		Id(coreVarName).Qual(cfg.GoCode.OutputPackage, apiName),
	)

	// ApplicationCore interface var
	f.Var().Id("_").Qual(monsteraPkg, "ApplicationCore").Op("=").Op("&").Id(adapterName).Values()

	// NewAdapter func
	f.Func().Id(
		"New"+adapterName,
	).Params(
		Id("shardId").String(),
		Id("replicaId").String(),
		Id(coreVarName).Qual(cfg.GoCode.OutputPackage, apiName),
	).Params(
		Op("*").Id(adapterName),
	).Block(
		Return(Op("&").Id(adapterName).Values(Dict{
			Id("shardId"):   Id("shardId"),
			Id("replicaId"): Id("replicaId"),
			Id(coreVarName): Id(coreVarName),
		})),
	)
	f.Line()

	// Snapshot func
	f.Func().Params(
		Id("a").Op("*").Id(adapterName),
	).Id("Snapshot").Params().Params(
		Qual(monsteraPkg, "ApplicationCoreSnapshot"),
	).Block(
		Defer().Id("measureSince").Call(
			Id("monsteraCoreMethodDuration").Dot("WithLabelValues").Call(
				Lit(core.Name),
				Lit("Snapshot"),
				Id("a").Dot("shardId"),
				Id("a").Dot("replicaId"),
			),
			Qual("time", "Now").Call(),
		),
		Id("monsteraCoreMethodCount").Dot("WithLabelValues").Call(
			Lit(core.Name),
			Lit("Snapshot"),
			Id("a").Dot("shardId"),
			Id("a").Dot("replicaId"),
		).Dot("Inc").Call(),
		Line(),
		Return(Id("a").Dot(coreVarName).Dot("Snapshot").Call()),
	)
	f.Line()

	// Restore func
	f.Func().Params(
		Id("a").Op("*").Id(adapterName),
	).Id("Restore").Params(
		Id("r").Qual("io", "ReadCloser"),
	).Params(
		Error(),
	).Block(
		Defer().Id("measureSince").Call(
			Id("monsteraCoreMethodDuration").Dot("WithLabelValues").Call(
				Lit(core.Name),
				Lit("Restore"),
				Id("a").Dot("shardId"),
				Id("a").Dot("replicaId"),
			),
			Qual("time", "Now").Call(),
		),
		Id("monsteraCoreMethodCount").Dot("WithLabelValues").Call(
			Lit(core.Name),
			Lit("Restore"),
			Id("a").Dot("shardId"),
			Id("a").Dot("replicaId"),
		).Dot("Inc").Call(),
		Line(),
		Return(Id("a").Dot(coreVarName).Dot("Restore").Call(Id("r"))),
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

			g.Id("t1").Op(":=").Qual("time", "Now").Call()
			g.Line()

			g.Switch(
				Id("rpcReq").Dot("MethodNumber"),
			).BlockFunc(func(g *Group) {
				for _, update := range core.UpdateMethods {
					g.Case(
						Lit(update.Number),
					).Block(
						Id("methodReq").Op(":=").Qual(corepb, update.Name+"Request").Op("{}"),
						Id("err").Op(":=").Id("methodReq").Dot("UnmarshalBinary").Call(
							Id("rpcReq").Dot("Data"),
						),
						If(
							Id("err").Op("!=").Nil(),
						).Block(
							Return(Nil(), Err()),
						),
						List(Id("methodResp"), Err()).Op(":=").Id("a").Dot(coreVarName).Dot(update.Name).Call(
							Op("&").Id(update.Name+"Request").Values(Dict{
								Id("Payload"): Op("&").Id("methodReq"),
								Id("Now"):     Id("rpcReq").Dot("Now"),
							}),
						),
						If(
							Id("err").Op("!=").Nil(),
						).Block(
							Return(Nil(), Err()),
						),
						Id("measureSince").Call(
							Id("monsteraCoreMethodDuration").Dot("WithLabelValues").Call(
								Lit(core.Name),
								Lit(update.Name),
								Id("a").Dot("shardId"),
								Id("a").Dot("replicaId"),
							),
							Id("t1"),
						),
						Id("monsteraCoreMethodCount").Dot("WithLabelValues").Call(
							Lit(core.Name),
							Lit(update.Name),
							Id("a").Dot("shardId"),
							Id("a").Dot("replicaId"),
						).Dot("Inc").Call(),
						Id("rpcResp").Dot("Error").Op("=").Id("methodResp").Dot("ApplicationError"),
						List(Id("methodRespBytes"), Err()).Op(":=").Id("methodResp").Dot("Payload").Dot("MarshalBinary").Call(),
						If(
							Id("err").Op("!=").Nil(),
						).Block(
							Return(Nil(), Err()),
						),
						Id("rpcResp").Dot("Data").Op("=").Id("methodRespBytes"),
						// Id("resp").Dot("Events").Op("=").Id("methodResp").Dot("Events"),
					)
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

			g.Id("t1").Op(":=").Qual("time", "Now").Call()
			g.Line()

			g.Switch(
				Id("rpcReq").Dot("MethodNumber"),
			).BlockFunc(func(g *Group) {
				for _, read := range core.ReadMethods {
					g.Case(
						Lit(read.Number),
					).Block(
						Id("methodReq").Op(":=").Qual(corepb, read.Name+"Request").Op("{}"),
						Id("err").Op(":=").Id("methodReq").Dot("UnmarshalBinary").Call(
							Id("rpcReq").Dot("Data"),
						),
						If(
							Id("err").Op("!=").Nil(),
						).Block(
							Return(Nil(), Err()),
						),
						List(Id("methodResp"), Err()).Op(":=").Id("a").Dot(coreVarName).Dot(read.Name).Call(
							Op("&").Id(read.Name+"Request").Values(Dict{
								Id("Payload"): Op("&").Id("methodReq"),
								Id("Now"):     Id("rpcReq").Dot("Now"),
							}),
						),
						If(
							Id("err").Op("!=").Nil(),
						).Block(
							Return(Nil(), Err()),
						),
						Id("measureSince").Call(
							Id("monsteraCoreMethodDuration").Dot("WithLabelValues").Call(
								Lit(core.Name),
								Lit(read.Name),
								Id("a").Dot("shardId"),
								Id("a").Dot("replicaId"),
							),
							Id("t1"),
						),
						Id("monsteraCoreMethodCount").Dot("WithLabelValues").Call(
							Lit(core.Name),
							Lit(read.Name),
							Id("a").Dot("shardId"),
							Id("a").Dot("replicaId"),
						).Dot("Inc").Call(),
						Id("rpcResp").Dot("Error").Op("=").Id("methodResp").Dot("ApplicationError"),
						List(Id("methodRespBytes"), Err()).Op(":=").Id("methodResp").Dot("Payload").Dot("MarshalBinary").Call(),
						If(
							Id("err").Op("!=").Nil(),
						).Block(
							Return(Nil(), Err()),
						),
						Id("rpcResp").Dot("Data").Op("=").Id("methodRespBytes"),
						// Id("resp").Dot("Events").Op("=").Id("methodResp").Dot("Events"),
					)
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
