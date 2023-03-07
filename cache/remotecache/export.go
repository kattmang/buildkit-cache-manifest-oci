package remotecache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/containerd/containerd/content"
	v1 "github.com/moby/buildkit/cache/remotecache/v1"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/compression"
	"github.com/moby/buildkit/util/contentutil"
	"github.com/moby/buildkit/util/progress"
	"github.com/moby/buildkit/util/progress/logs"
	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

type ResolveCacheExporterFunc func(ctx context.Context, g session.Group, attrs map[string]string) (Exporter, error)

type Exporter interface {
	solver.CacheExporterTarget
	// Name uniquely identifies the exporter
	Name() string
	// Finalize finalizes and return metadata that are returned to the client
	// e.g. ExporterResponseManifestDesc
	Finalize(ctx context.Context) (map[string]string, error)
	Config() Config
}

type Config struct {
	Compression compression.Config
}

const (
	// ExportResponseManifestDesc is a key for the map returned from Exporter.Finalize.
	// The map value is a JSON string of an OCI desciptor of a manifest.
	ExporterResponseManifestDesc = "cache.manifest"
)

func NewExporter(ingester content.Ingester, ref string, oci bool, compressionConfig compression.Config) Exporter {
	cc := v1.NewCacheChains()
	return &artifactContentCacheExporter{CacheExporterTarget: cc, chains: cc, ingester: ingester, oci: oci, ref: ref, comp: compressionConfig}
}

//TODO: readd contentCacheExporter and refactor to not need an alternative/child exporter
//TODO: add flag (maybe replace OCI flag?)

// Below is an OCI 1.1-compliant "Image Manifest as Artifact" approach
type artifactContentCacheExporter struct {
	solver.CacheExporterTarget
	chains   *v1.CacheChains
	ingester content.Ingester
	oci      bool
	ref      string
	comp     compression.Config
}

func (ce *artifactContentCacheExporter) Name() string {
	return "exporting content cache"
}

func (ce *artifactContentCacheExporter) Config() Config {
	return Config{
		Compression: ce.comp,
	}
}

func (ce *artifactContentCacheExporter) Finalize(ctx context.Context) (map[string]string, error) {
	res := make(map[string]string)
	config, descs, err := ce.chains.Marshal(ctx)
	if err != nil {
		return nil, err
	}

	// own type because oci type can't be pushed and docker type doesn't have annotations
	type manifestImageArtifact struct {
		specs.Versioned
		MediaType string `json:"mediaType,omitempty"`

		Config ocispecs.Descriptor `json:"config"`

		Layers []ocispecs.Descriptor `json:"layers"`
	}

	var mfst manifestImageArtifact
	mfst.SchemaVersion = 2
	mfst.MediaType = ocispecs.MediaTypeImageManifest

	for _, l := range config.Layers {
		dgstPair, ok := descs[l.Blob]
		if !ok {
			return nil, errors.Errorf("missing blob %s", l.Blob)
		}
		layerDone := progress.OneOff(ctx, fmt.Sprintf("writing layer %s", l.Blob))
		if err := contentutil.Copy(ctx, ce.ingester, dgstPair.Provider, dgstPair.Descriptor, ce.ref, logs.LoggerFromContext(ctx)); err != nil {
			return nil, layerDone(errors.Wrap(err, "error writing layer blob"))
		}
		layerDone(nil)
		mfst.Layers = append(mfst.Layers, dgstPair.Descriptor)
	}

	//mfst.Layers = compression.ConvertAllLayerMediaTypes(ce.oci, mfst.Manifests...)

	dt, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	dgst := digest.FromBytes(dt)
	desc := ocispecs.Descriptor{
		Digest:    dgst,
		Size:      int64(len(dt)),
		MediaType: v1.CacheConfigMediaTypeV0,
	}
	configDone := progress.OneOff(ctx, fmt.Sprintf("writing config %s", dgst))
	if err := content.WriteBlob(ctx, ce.ingester, dgst.String(), bytes.NewReader(dt), desc); err != nil {
		return nil, configDone(errors.Wrap(err, "error writing config blob"))
	}
	configDone(nil)

	mfst.Config = desc

	dt, err = json.Marshal(mfst)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal manifest")
	}
	dgst = digest.FromBytes(dt)

	desc = ocispecs.Descriptor{
		Digest:    dgst,
		Size:      int64(len(dt)),
		MediaType: mfst.MediaType,
	}

	mfstDone := progress.OneOff(ctx, fmt.Sprintf("writing kangmatt's manifest %s", dgst))
	if err := content.WriteBlob(ctx, ce.ingester, dgst.String(), bytes.NewReader(dt), desc); err != nil {
		return nil, mfstDone(errors.Wrap(err, "error writing manifest blob"))
	}
	descJSON, err := json.Marshal(desc)
	if err != nil {
		return nil, err
	}
	res[ExporterResponseManifestDesc] = string(descJSON)
	mfstDone(nil)
	return res, nil
}
