package codegen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// validYaml returns a valid manifest with two cores and two stubs that share
// CoreA — the sharing exercises the once-per-file emission of shared
// declarations.
func validYaml() *MonsteraYaml {
	return &MonsteraYaml{
		GoCode: &GoCode{
			OutputPackage:    "github.com/example/app/codegen",
			CoreTypesPackage: "github.com/example/app/corepb",
		},
		Cores: []*MonsteraCore{
			{
				Name: "CoreA",
				ReadMethods: []*ReadMethod{
					{Name: "GetA", Number: 1, Sharded: true},
					{Name: "ListA", Number: 2, Sharded: false},
				},
				UpdateMethods: []*UpdateMethod{
					{Name: "PutA", Number: 1, Sharded: true},
				},
			},
			{
				Name: "CoreB",
				ReadMethods: []*ReadMethod{
					{Name: "GetB", Number: 1, Sharded: true},
				},
				UpdateMethods: []*UpdateMethod{
					{Name: "PutB", Number: 1, Sharded: false},
				},
			},
		},
		Stubs: []*MonsteraStub{
			{Name: "First", Cores: []string{"CoreA", "CoreB"}},
			{Name: "Second", Cores: []string{"CoreA"}},
		},
	}
}

func TestValidateAcceptsValidManifest(t *testing.T) {
	require.NoError(t, validYaml().Validate())
}

func TestValidateRejectsInvalidManifests(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(cfg *MonsteraYaml)
		wantErr string
	}{
		{
			name:    "missing go_code",
			mutate:  func(cfg *MonsteraYaml) { cfg.GoCode = nil },
			wantErr: "go_code section is required",
		},
		{
			name:    "missing output_package",
			mutate:  func(cfg *MonsteraYaml) { cfg.GoCode.OutputPackage = "" },
			wantErr: "output_package is required",
		},
		{
			name: "equal packages",
			mutate: func(cfg *MonsteraYaml) {
				cfg.GoCode.CoreTypesPackage = cfg.GoCode.OutputPackage
			},
			wantErr: "must be different packages",
		},
		{
			name:    "no cores",
			mutate:  func(cfg *MonsteraYaml) { cfg.Cores = nil },
			wantErr: "at least one core is required",
		},
		{
			name:    "duplicate core name",
			mutate:  func(cfg *MonsteraYaml) { cfg.Cores[1].Name = "CoreA" },
			wantErr: "duplicate core",
		},
		{
			name:    "core name not an exported identifier",
			mutate:  func(cfg *MonsteraYaml) { cfg.Cores[0].Name = "get-lock" },
			wantErr: "must be a valid exported Go identifier",
		},
		{
			name:    "method name not an exported identifier",
			mutate:  func(cfg *MonsteraYaml) { cfg.Cores[0].ReadMethods[0].Name = "getA" },
			wantErr: "must be a valid exported Go identifier",
		},
		{
			name:    "missing method_number",
			mutate:  func(cfg *MonsteraYaml) { cfg.Cores[0].ReadMethods[0].Number = 0 },
			wantErr: "needs an explicit method_number",
		},
		{
			name:    "duplicate read method_number in a core",
			mutate:  func(cfg *MonsteraYaml) { cfg.Cores[0].ReadMethods[1].Number = 1 },
			wantErr: "share method_number 1",
		},
		{
			name:    "duplicate method name across cores",
			mutate:  func(cfg *MonsteraYaml) { cfg.Cores[1].ReadMethods[0].Name = "GetA" },
			wantErr: "unique across all cores",
		},
		{
			name:    "duplicate method name across kinds",
			mutate:  func(cfg *MonsteraYaml) { cfg.Cores[0].UpdateMethods[0].Name = "GetA" },
			wantErr: "unique across all cores",
		},
		{
			name:    "duplicate stub name",
			mutate:  func(cfg *MonsteraYaml) { cfg.Stubs[1].Name = "First" },
			wantErr: "duplicate stub",
		},
		{
			name:    "stub with no cores",
			mutate:  func(cfg *MonsteraYaml) { cfg.Stubs[0].Cores = nil },
			wantErr: "lists no cores",
		},
		{
			name:    "stub references unknown core",
			mutate:  func(cfg *MonsteraYaml) { cfg.Stubs[0].Cores = []string{"Nope"} },
			wantErr: "unknown core",
		},
		{
			name:    "stub lists a core twice",
			mutate:  func(cfg *MonsteraYaml) { cfg.Stubs[0].Cores = []string{"CoreA", "CoreA"} },
			wantErr: "more than once",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validYaml()
			tt.mutate(cfg)
			err := cfg.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestLoadMonsteraYamlRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "monstera.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
go_code:
  output_package: github.com/example/app/codegen
  core_types_package: github.com/example/app/corepb
cores:
  - name: CoreA
    read_methods:
      - name: GetA
        method_number: 1
        sharded: true
        allow_read_from_folowers: true
`), 0644))

	_, err := LoadMonsteraYaml(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "allow_read_from_folowers")
}

func TestLoadMonsteraYamlRejectsMissingGoCode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "monstera.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
cores:
  - name: CoreA
`), 0644))

	_, err := LoadMonsteraYaml(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "go_code section is required")
}

func TestLoadMonsteraYamlRejectsEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "monstera.yaml")
	require.NoError(t, os.WriteFile(path, nil, 0644))

	_, err := LoadMonsteraYaml(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty")
}

// TestGenerateStubsSharedDeclarationsEmittedOnce guards against generating
// uncompilable code when several stubs exist and share a core: package-level
// helpers and per-core adapter types must be emitted exactly once per file.
func TestGenerateStubsSharedDeclarationsEmittedOnce(t *testing.T) {
	out, err := GenerateStubs(validYaml())
	require.NoError(t, err)

	require.EqualValues(t, 1, strings.Count(out, "func nilifyIfEmpty"))
	require.EqualValues(t, 1, strings.Count(out, "type coreACoreNonclusteredAdapter struct"))
	require.EqualValues(t, 1, strings.Count(out, "type coreBCoreNonclusteredAdapter struct"))

	// Per-stub declarations are still emitted for every stub.
	require.Contains(t, out, "type FirstMonsteraStub struct")
	require.Contains(t, out, "type SecondMonsteraStub struct")
	require.Contains(t, out, "type FirstNonclusteredStub struct")
	require.Contains(t, out, "type SecondNonclusteredStub struct")
}

func TestGenerateStubsUnknownCore(t *testing.T) {
	cfg := validYaml()
	cfg.Stubs[0].Cores = []string{"Nope"} // bypasses Validate on purpose

	_, err := GenerateStubs(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), `unknown core "Nope"`)
}

func TestGenerateCoreApisUnknownCore(t *testing.T) {
	cfg := validYaml()
	cfg.Stubs[0].Cores = []string{"Nope"} // bypasses Validate on purpose

	_, err := GenerateCoreApis(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), `unknown core "Nope"`)
}

// TestGeneratedAdapterGuardsNilPayload checks that the generated adapter only
// marshals the method response payload when it is non-nil: a core returning
// only an ApplicationError must not panic the Raft apply path.
func TestGeneratedAdapterGuardsNilPayload(t *testing.T) {
	out, err := GenerateAdapters(validYaml())
	require.NoError(t, err)

	require.NotContains(t, out, "methodRespBytes, err := methodResp.Payload.MarshalBinary()\n\tif")
	require.Contains(t, out, "if methodResp.Payload != nil {")
}

// TestGeneratedNonclusteredStubValidatesShardKeys checks that the nonclustered
// stub enforces the same 1-4 byte shard key contract as the clustered path
// (cluster.Config.FindShardByShardKey), so invalid keys fail in dev, not on
// deploy.
func TestGeneratedNonclusteredStubValidatesShardKeys(t *testing.T) {
	out, err := GenerateStubs(validYaml())
	require.NoError(t, err)

	require.Contains(t, out, "invalid shard key length %d: must be between 1 and 4 bytes")
	require.Contains(t, out, "shardsPerApp must be a power of 2 between 1 and 2^32")
}
