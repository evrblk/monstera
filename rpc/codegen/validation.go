package codegen

import (
	"fmt"

	. "github.com/dave/jennifer/jen" //lint:ignore ST1001 jen helpers are so much nicer to use with dot-importing
)

// GenerateValidation generates, for each core, a XxxValidatingCore that
// implements XxxCoreApi by wrapping another XxxCoreApi: every Read/Update
// method first calls Validate on the request payload, rejecting it with an
// ApplicationError instead of delegating if it fails. Adapter constructors
// wrap the core passed to them in one of these, so a request reaching
// Core.<Method> is guaranteed to have already passed Validate — no call site
// of the adapter constructor can opt out of it.
func GenerateValidation(cfg *MonsteraYaml) (string, error) {
	f := NewFilePath(cfg.GoCode.OutputPackage)
	f.HeaderComment(generatedCodeComment)
	f.ImportAlias(mrpcPkg, "mrpc")

	for _, core := range cfg.Cores {
		generateValidatingCore(f, core, cfg)
	}

	return fmt.Sprintf("%#v", f), nil
}

func generateValidatingCore(f *File, core *MonsteraCore, cfg *MonsteraYaml) {
	apiName := core.Name + "CoreApi"
	validatingName := core.Name + "ValidatingCore"
	coreVarName := "core"

	f.Type().Id(validatingName).Struct(
		Id(coreVarName).Qual(cfg.GoCode.OutputPackage, apiName),
	)

	f.Var().Id("_").Qual(cfg.GoCode.OutputPackage, apiName).Op("=").Op("&").Id(validatingName).Values()
	f.Line()

	f.Func().Id("New"+validatingName).Params(
		Id(coreVarName).Qual(cfg.GoCode.OutputPackage, apiName),
	).Params(
		Op("*").Id(validatingName),
	).Block(
		Return(Op("&").Id(validatingName).Values(Dict{
			Id(coreVarName): Id(coreVarName),
		})),
	)
	f.Line()

	recv := Id("v").Op("*").Id(validatingName)

	f.Func().Params(recv).Id("Snapshot").Params().Params(
		Qual(monsteraPkg, "ApplicationCoreSnapshot"),
	).Block(
		Return(Id("v").Dot(coreVarName).Dot("Snapshot").Call()),
	)
	f.Line()

	f.Func().Params(recv).Id("Restore").Params(
		Id("readers").Op("...").Qual("io", "ReadCloser"),
	).Params(
		Error(),
	).Block(
		Return(Id("v").Dot(coreVarName).Dot("Restore").Call(Id("readers").Op("..."))),
	)
	f.Line()

	f.Func().Params(recv).Id("Close").Params().Block(
		Id("v").Dot(coreVarName).Dot("Close").Call(),
	)
	f.Line()

	methodNames := make([]string, 0, len(core.ReadMethods)+len(core.UpdateMethods))
	for _, read := range core.ReadMethods {
		methodNames = append(methodNames, read.Name)
	}
	for _, update := range core.UpdateMethods {
		methodNames = append(methodNames, update.Name)
	}

	for _, methodName := range methodNames {
		f.Func().Params(recv).Id(methodName).Params(
			Id("req").Op("*").Qual(cfg.GoCode.OutputPackage, methodName+"Request"),
		).Params(
			List(
				Op("*").Qual(cfg.GoCode.OutputPackage, methodName+"Response"),
				Error(),
			),
		).Block(
			If(
				Err().Op(":=").Id("req").Dot("Payload").Dot("Validate").Call(),
				Err().Op("!=").Nil(),
			).Block(
				Return(
					Op("&").Qual(cfg.GoCode.OutputPackage, methodName+"Response").Values(Dict{
						Id("ApplicationError"): Qual(mrpcPkg, "NewError").Call(
							Qual(mrpcPkg, "InvalidRequest"),
							Err().Dot("Error").Call(),
						),
					}),
					Nil(),
				),
			),
			Return(Id("v").Dot(coreVarName).Dot(methodName).Call(Id("req"))),
		)
		f.Line()
	}
}
