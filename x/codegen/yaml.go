package codegen

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type MonsteraYaml struct {
	Cores  []*MonsteraCore `yaml:"cores"`
	Stubs  []*MonsteraStub `yaml:"stubs"`
	GoCode *GoCode         `yaml:"go_code"`
}

type GoCode struct {
	OutputPackage    string `yaml:"output_package"`
	CoreTypesPackage string `yaml:"core_types_package"`
}

type MonsteraCore struct {
	Name          string          `yaml:"name"`
	ReadMethods   []*ReadMethod   `yaml:"read_methods"`
	UpdateMethods []*UpdateMethod `yaml:"update_methods"`
}

type MonsteraStub struct {
	Name  string   `yaml:"name"`
	Cores []string `yaml:"cores"`
}

type ReadMethod struct {
	Name                   string `yaml:"name"`
	AllowReadFromFollowers bool   `yaml:"allow_read_from_followers"`
	Sharded                bool   `yaml:"sharded"`
	Number                 int    `yaml:"method_number"`
}

type UpdateMethod struct {
	Name    string `yaml:"name"`
	Sharded bool   `yaml:"sharded"`
	Number  int    `yaml:"method_number"`
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
