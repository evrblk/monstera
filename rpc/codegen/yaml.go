package codegen

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

// MonsteraYaml is the root of a monstera.yaml manifest — the input of
// `monstera code generate`. It declares the application cores with their RPC
// methods, the client stubs that group those cores, and where the generated
// Go code goes. Load it with LoadMonsteraYaml; hand-built values must pass
// Validate before being fed to the generators.
type MonsteraYaml struct {
	// Cores declares the application cores. At least one is required.
	Cores []*MonsteraCore `yaml:"cores"`

	// Stubs declares the client stubs. Optional: with no stubs only the core
	// APIs and adapters are generated.
	Stubs []*MonsteraStub `yaml:"stubs"`

	// GoCode tells the generator which Go packages the generated code lives
	// in and references. Required.
	GoCode *GoCode `yaml:"go_code"`
}

// GoCode locates the generated code and the application's payload types.
type GoCode struct {
	// OutputPackage is the import path of the package the generated files
	// (stubs.go, api.go, adapters.go) belong to — the directory
	// `monstera code generate` runs in.
	OutputPackage string `yaml:"output_package"`

	// CoreTypesPackage is the import path of the package holding the
	// application's *Request/*Response payload types. It must differ from
	// OutputPackage: the generated type aliases would otherwise collide with
	// the payload types.
	CoreTypesPackage string `yaml:"core_types_package"`
}

// MonsteraCore declares one application core and its RPC methods. The
// generator produces a <Name>CoreApi interface (what the application
// implements) and a <Name>CoreAdapter that dispatches the framework's RPC
// envelope to it.
type MonsteraCore struct {
	// Name of the core, a valid exported Go identifier, unique within the
	// manifest. It also names the application in the cluster config: the
	// clustered stub routes requests using it.
	Name string `yaml:"name"`

	// ReadMethods are the core's read-only methods (served without going
	// through Raft).
	ReadMethods []*ReadMethod `yaml:"read_methods"`

	// UpdateMethods are the core's mutating methods (replicated through
	// Raft).
	UpdateMethods []*UpdateMethod `yaml:"update_methods"`
}

// MonsteraStub declares one client stub: the union of the methods of the
// cores it lists, as a <Name>ClientApi interface with two generated
// implementations — <Name>MonsteraStub (routes through the Monstera client)
// and <Name>NonclusteredStub (drives the cores in-process, for single-node
// mode and tests).
type MonsteraStub struct {
	// Name of the stub, a valid exported Go identifier, unique within the
	// manifest.
	Name string `yaml:"name"`

	// Cores lists the names of the cores whose methods this stub exposes.
	// Every entry must name a declared core; a core may appear in several
	// stubs but only once per stub.
	Cores []string `yaml:"cores"`
}

// ReadMethod declares one read-only method of a core.
type ReadMethod struct {
	// Name of the method, a valid exported Go identifier. Method names must
	// be unique across all cores of the manifest: each becomes a
	// package-level *Request/*Response type alias in the output package.
	Name string `yaml:"name"`

	// AllowReadFromFollowers permits serving this method from follower
	// replicas, for reads that tolerate slightly stale data. Defaults to
	// false (leader-only reads).
	AllowReadFromFollowers bool `yaml:"allow_read_from_followers"`

	// Sharded routes the request to a single shard by the request's
	// ShardKey(). When false, the method targets a shard by id explicitly
	// (cluster-wide or fan-out operations) and takes an extra shardId
	// argument.
	Sharded bool `yaml:"sharded"`

	// Number identifies the method on the wire, like a protobuf field
	// number: explicit (>= 1), unique among the core's read methods, and
	// never changed once in use.
	Number int `yaml:"method_number"`
}

// UpdateMethod declares one mutating method of a core.
type UpdateMethod struct {
	// Name of the method, a valid exported Go identifier. Method names must
	// be unique across all cores of the manifest: each becomes a
	// package-level *Request/*Response type alias in the output package.
	Name string `yaml:"name"`

	// Sharded routes the request to a single shard by the request's
	// ShardKey(). When false, the method targets a shard by id explicitly
	// and takes an extra shardId argument.
	Sharded bool `yaml:"sharded"`

	// Number identifies the method on the wire, like a protobuf field
	// number: explicit (>= 1), unique among the core's update methods, and
	// never changed once in use.
	Number int `yaml:"method_number"`
}

// LoadMonsteraYaml reads and validates a monstera.yaml manifest. Decoding is
// strict, and the manifest is validated before it is returned, so the generators
// can assume a well-formed config.
func LoadMonsteraYaml(path string) (*MonsteraYaml, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", path, err)
	}

	monsteraYaml := &MonsteraYaml{}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(monsteraYaml); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%s is empty", path)
		}
		return nil, fmt.Errorf("failed to parse %s: %w", path, err)
	}

	if err := monsteraYaml.Validate(); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", path, err)
	}

	return monsteraYaml, nil
}

// exportedIdentifier matches names the generator turns into exported Go
// identifiers (types, interface methods).
var exportedIdentifier = regexp.MustCompile(`^[A-Z][A-Za-z0-9_]*$`)

// Validate checks everything the generators assume about the manifest: names
// must form valid exported Go identifiers, method names must be unique across
// all cores (each becomes a package-level type alias), method numbers must be
// explicit and unique per kind within a core, stubs must reference declared
// cores, and the two output packages must differ. Violations are reported with
// enough context to fix the YAML, instead of surfacing as compile errors in
// generated files.
func (cfg *MonsteraYaml) Validate() error {
	if cfg.GoCode == nil {
		return fmt.Errorf("go_code section is required")
	}
	if cfg.GoCode.OutputPackage == "" {
		return fmt.Errorf("go_code.output_package is required")
	}
	if cfg.GoCode.CoreTypesPackage == "" {
		return fmt.Errorf("go_code.core_types_package is required")
	}
	if cfg.GoCode.OutputPackage == cfg.GoCode.CoreTypesPackage {
		return fmt.Errorf("go_code.output_package and go_code.core_types_package must be different packages (both are %q): generated *Request/*Response aliases would collide with the payload types", cfg.GoCode.OutputPackage)
	}

	if len(cfg.Cores) == 0 {
		return fmt.Errorf("at least one core is required")
	}

	coreNames := make(map[string]bool)
	// Method names must be unique across all cores, not just within one:
	// every method becomes a package-level *Request/*Response type alias in
	// output_package.
	methodOwners := make(map[string]string)
	for _, core := range cfg.Cores {
		if err := validateName("core", core.Name); err != nil {
			return err
		}
		if coreNames[core.Name] {
			return fmt.Errorf("duplicate core %q", core.Name)
		}
		coreNames[core.Name] = true

		readNumbers := make(map[int]string)
		for _, m := range core.ReadMethods {
			if err := validateMethod(core.Name, "read", m.Name, m.Number, readNumbers, methodOwners); err != nil {
				return err
			}
		}
		updateNumbers := make(map[int]string)
		for _, m := range core.UpdateMethods {
			if err := validateMethod(core.Name, "update", m.Name, m.Number, updateNumbers, methodOwners); err != nil {
				return err
			}
		}
	}

	stubNames := make(map[string]bool)
	for _, stub := range cfg.Stubs {
		if err := validateName("stub", stub.Name); err != nil {
			return err
		}
		if stubNames[stub.Name] {
			return fmt.Errorf("duplicate stub %q", stub.Name)
		}
		stubNames[stub.Name] = true

		if len(stub.Cores) == 0 {
			return fmt.Errorf("stub %q lists no cores", stub.Name)
		}
		seen := make(map[string]bool)
		for _, coreName := range stub.Cores {
			if !coreNames[coreName] {
				return fmt.Errorf("stub %q references unknown core %q", stub.Name, coreName)
			}
			if seen[coreName] {
				return fmt.Errorf("stub %q lists core %q more than once", stub.Name, coreName)
			}
			seen[coreName] = true
		}
	}

	return nil
}

func validateName(what string, name string) error {
	if name == "" {
		return fmt.Errorf("%s name is required", what)
	}
	if !exportedIdentifier.MatchString(name) {
		return fmt.Errorf("%s name %q must be a valid exported Go identifier (%s)", what, name, exportedIdentifier)
	}
	return nil
}

// validateMethod checks one method declaration. numbers tracks method numbers
// already used by the same kind (read/update) in the same core; methodOwners
// tracks method names across all cores.
func validateMethod(coreName string, kind string, name string, number int, numbers map[int]string, methodOwners map[string]string) error {
	if err := validateName(kind+" method", name); err != nil {
		return fmt.Errorf("core %q: %w", coreName, err)
	}
	if owner, ok := methodOwners[name]; ok {
		return fmt.Errorf("core %q: method %q is already declared by core %q: method names must be unique across all cores (each becomes a package-level generated type)", coreName, name, owner)
	}
	methodOwners[name] = coreName

	if number < 1 {
		return fmt.Errorf("core %q: %s method %q needs an explicit method_number >= 1 (it identifies the method on the wire and must never change)", coreName, kind, name)
	}
	if other, ok := numbers[number]; ok {
		return fmt.Errorf("core %q: %s methods %q and %q share method_number %d", coreName, kind, other, name, number)
	}
	numbers[number] = name

	return nil
}
