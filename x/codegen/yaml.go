package codegen

import (
	"fmt"
	"os"

	"github.com/samber/lo"
	"gopkg.in/yaml.v3"
)

type MonsteraYaml struct {
	Cores  []*MonsteraCore `yaml:"cores"`
	Stubs  []*MonsteraStub `yaml:"stubs"`
	GoCode *GoCode         `yaml:"go_code"`
}

type GoCode struct {
	OutputPackage string `yaml:"output_package"`
	CorePbPackage string `yaml:"corepb_package"`
}

type MonsteraCore struct {
	Name                string          `yaml:"name"`
	Reads               []*ReadMethod   `yaml:"reads"`
	Updates             []*UpdateMethod `yaml:"updates"`
	UpdateRequestProto  string          `yaml:"update_request_proto"`
	UpdateResponseProto string          `yaml:"update_response_proto"`
	ReadRequestProto    string          `yaml:"read_request_proto"`
	ReadResponseProto   string          `yaml:"read_response_proto"`
}

type MonsteraStub struct {
	Name  string   `yaml:"name"`
	Cores []string `yaml:"cores"`
}

type ReadMethod struct {
	Name                   string `yaml:"method"`
	AllowReadFromFollowers bool   `yaml:"allow_read_from_followers"`
	Sharded                bool   `yaml:"sharded"`
}

type UpdateMethod struct {
	Name    string `yaml:"method"`
	Sharded bool   `yaml:"sharded"`
}

func LoadMonsteraYaml(path string) (*MonsteraYaml, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %v", path, err)
	}

	monsteraYaml := &MonsteraYaml{}
	err = yaml.Unmarshal(data, monsteraYaml)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s: %v", path, err)
	}

	return monsteraYaml, nil
}

func getAllMethods(core *MonsteraCore) []string {
	return append(lo.Map(core.Reads, func(read *ReadMethod, _ int) string {
		return read.Name
	}), lo.Map(core.Updates, func(update *UpdateMethod, _ int) string {
		return update.Name
	})...)
}
