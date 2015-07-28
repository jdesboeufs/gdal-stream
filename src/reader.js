import { Readable } from 'readable-stream';
import { parse as parseSrs } from 'srs';
import gdal from 'gdal';


const wgs84 = gdal.SpatialReference.fromEPSG(4326);

export default class Reader extends Readable {

    constructor(location, options = {}) {
        super({ objectMode: true });

        this.location = location;
        this.options = options;
        this.readFeatureCount = 0;

        this.ds = gdal.open(location);
        this.layer = this.ds.layers.get(0);
    }

    ensureWGS84(geometry) {
        if (this.srsName() !== 'WGS 84') {
            geometry.transformTo(wgs84);
        }
        return geometry.toObject();
    }

    srsName() {
        if (this.metadata().sourceSrs && this.metadata().sourceSrs.name) {
            return this.metadata().sourceSrs.name;
        }
    }

    // Implement ReadableStream interface
    _read() {
        setImmediate(() => {
            let feature = this.layer.features.next();
            if (feature) {
                this.push({
                    type: 'Feature',
                    properties: feature.fields.toObject(),
                    geometry: this.ensureWGS84(feature.getGeometry())
                });
                this.readFeatureCount++;
            } else {
                this.push(null);
            }
        });
    }

    metadata() {
        if (!this.metadataCache) {
            this.metadataCache = {
                featureCount: this.layer.features.count(),
                fields: this.layer.fields.getNames(),
                sourceSrs: (this.layer.srs ? parseSrs(this.layer.srs.toWKT()) : null),
                extent: this.layer.getExtent(),
                files: this.ds.getFileList(),
                layerName: this.layer.name,
                fidColumn: this.layer.fidColumn,
                geomColumn: this.layer.geomColumn,
                geomType: gdal.Geometry.getName(this.layer.geomType)
            };
        }
        return this.metadataCache;
    }

}
