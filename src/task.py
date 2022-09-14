import argparse
import ee
from task_base import SCLTask


class SCLStats(SCLTask):
    ee_rootdir = "projects/SCL/v1"
    inputs = {
        "scl_image": {
            "ee_type": SCLTask.IMAGECOLLECTION,
            "ee_path": "scl_image_path",
            "maxage": 1 / 365,
        },
        "structural_habitat": {
            "ee_type": SCLTask.IMAGECOLLECTION,
            "ee_path": "structural_habitat_path",
            "maxage": 1 / 365,
        },
        "occupied_habitat_image": {
            "ee_type": SCLTask.IMAGECOLLECTION,
            "ee_path": "occupied_image_habitat_path",
            "maxage": 1 / 365,
        },
        "scl_species": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": f"scl_path_{SCLTask.SPECIES}",
            "maxage": 1 / 365,  # years
        },
        "scl_restoration": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": f"scl_path_{SCLTask.RESTORATION}",
            "maxage": 1 / 365,
        },
        "scl_survey": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": f"scl_path_{SCLTask.SURVEY}",
            "maxage": 1 / 365,
        },
        "scl_species_fragment": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": f"scl_path_{SCLTask.SPECIES}_{SCLTask.FRAGMENT}",
            "maxage": 1 / 365,  # years
        },
        "scl_restoration_fragment": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": f"scl_path_{SCLTask.RESTORATION}_{SCLTask.FRAGMENT}",
            "maxage": 1 / 365,
        },
        "scl_survey_fragment": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": f"scl_path_{SCLTask.SURVEY}_{SCLTask.FRAGMENT}",
            "maxage": 1 / 365,
        },
        "kbas": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": "projects/SCL/v1/source/KBAsGlobal_20200301",
            "static": True,
        },
        "states": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": "projects/SCL/v1/source/gadm404_state_simp",
            "static": True,
        },
    }
    output_properties = {
        "landscape": {"selectors": ["dissolved_poly_id"], "rename": ["lsid"]},
        "country": {
            "selectors": ["dissolved_poly_id", "countrynam", "iso2", "isonumeric"],
            "rename": ["lsid", "country", "iso2", "isonumeric"],
        },
        "state": {
            "selectors": [
                "dissolved_poly_id",
                "countrynam",
                "iso2",
                "isonumeric",
                "gadm1name",
                "gadm1code",
            ],
            "rename": ["lsid", "country", "iso2", "isonumeric", "state", "gadm1code"],
        },
    }

    def rounded_habitat_area(self, geom, img=None, band=None, scale=None):
        img = img or self.scl_image.select("eff_pot_hab_area")  # assumes unit=km2
        band = band or "eff_pot_hab_area"
        scale = scale or self.scale

        stats = img.reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            scale=scale,
            maxPixels=self.ee_max_pixels,
        )
        return ee.Number(stats.get(band)).multiply(10).round().multiply(0.1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.scenario != self.CANONICAL:
            raise NotImplementedError(
                "Generating statistics for non-canonical species landscape calculations not implemented until "
                "there is a consumer or use case defined for it, as SCL API ingestion is for canonical."
            )
        self.error_margin = ee.ErrorMargin(1)
        self.scl_image, _ = self.get_most_recent_image(
            ee.ImageCollection(self.inputs["scl_image"]["ee_path"])
        )
        self.structural_habitat, _ = self.get_most_recent_image(
            ee.ImageCollection(self.inputs["structural_habitat"]["ee_path"])
        )
        self.occupied_habitat_image, _ = self.get_most_recent_image(
            ee.ImageCollection(self.inputs["occupied_habitat_image"]["ee_path"])
        )
        self.kbas = ee.FeatureCollection(self.inputs["kbas"]["ee_path"])
        self.states = ee.FeatureCollection(self.inputs["states"]["ee_path"])
        self.area_image = None

    def scl_image_path(self):
        return f"{self.ee_rootdir}/pothab/scl_image"

    def structural_habitat_path(self):
        return f"{self.ee_rootdir}/structural_habitat"

    def occupied_image_habitat_path(self):
        return f"{self.ee_rootdir}/pothab/occupied_habitat"

    def landscapes_fc_interections(self, landscapes, landscape_key, fc, type):
        def intersections(landscape):
            ls_features = fc.filterBounds(landscape.geometry())

            def ls_feature_intersection(feature):
                ls_feature_intersection = landscape.geometry().intersection(
                    feature.geometry(), self.error_margin
                )
                properties = self.output_properties[type]["selectors"]
                new_properties = self.output_properties[type]["rename"]

                intersection_feature = (
                    ee.Feature(ls_feature_intersection)
                    .copyProperties(landscape)
                    .copyProperties(feature)
                )

                return (
                    ee.Feature(intersection_feature)
                    .select(properties, new_properties)
                    .set({"ls_key": landscape_key,})
                )

            return ls_features.map(ls_feature_intersection)

        return landscapes.map(intersections).flatten()

    def calc_landscape_geometries(
        self, landscape_key, feature_collection, type, export
    ):
        landscapes, landscapes_date = self.get_most_recent_featurecollection(
            self.inputs[landscape_key]["ee_path"]
        )

        if landscapes is None:
            return

        if type == "landscape":
            landscape_geometries = landscapes

        else:
            landscape_geometries = self.landscapes_fc_interections(
                landscapes, landscape_key, feature_collection, type
            )

        if export:
            bucket = self.gcsclient.get_bucket(self.DEFAULT_BUCKET)
            blob = f"ls_geometries/{self.species}/{self.scenario}/{self.taskdate}/by_{type}/{landscape_key}"
            self.table2storage(landscape_geometries, self.DEFAULT_BUCKET, blob)
            return

        return landscape_geometries

    def calc_landscape_stats(self, landscape_key):
        bucket = self.gcsclient.get_bucket(self.DEFAULT_BUCKET)
        blob = f"ls_stats/{self.species}/{self.scenario}/{self.taskdate}/scl_{landscape_key}"

        landscapes = self.calc_landscape_geometries(
            f"scl_{landscape_key}", self.states, "state", False
        )

        def habitat_areas(feat):
            area_stats = self.area_image.reduceRegion(
                geometry=feat.geometry(),
                reducer=ee.Reducer.sum(),
                scale=self.scale,
                crs=self.crs,
                maxPixels=self.ee_max_pixels,
            )
            return {
                "total_area": ee.Number(area_stats.get("total_area")).round(),
                "indigenous_range_area": ee.Number(
                    area_stats.get("indigenous_range_area")
                ).round(),
                "str_hab_area": ee.Number(area_stats.get("str_hab_area")).round(),
                "eff_pot_hab_area": ee.Number(
                    area_stats.get("eff_pot_hab_area")
                ).round(),
                "connected_eff_pot_hab_area": ee.Number(
                    area_stats.get("connected_eff_pot_hab_area")
                ).round(),
                "occupied_eff_pot_hab_area": ee.Number(
                    area_stats.get("occupied_eff_pot_hab_area")
                ).round(),
            }

        def feature_area_stats(feat):
            ls_state_stats = feat.set(habitat_areas(feat))

            def ecoregion_stats(ecoregion):
                ls_ecoregion = ee.Feature(
                    ecoregion.geometry().intersection(feat.geometry())
                )

                ecoregion_area_stats = habitat_areas(ls_ecoregion)

                ecoregion_properties = {
                    "biome_name": ecoregion.get("BIOME_NAME"),
                    "biome_id": ecoregion.get("BIOME_NUM"),
                    "ecoregion_name": ecoregion.get("ECO_NAME"),
                    "ecoregion_id": ecoregion.get("ECO_ID"),
                }
                stats = {**ecoregion_properties, **ecoregion_area_stats}

                return ee.Feature(None, {"stats": stats})

            def kba_stats(kba):
                ls_kba = ee.Feature(kba.geometry().intersection(feat.geometry()))
                kba_area_stats = habitat_areas(ls_kba)
                kba_properties = {
                    "kba_name": kba.get("IntName"),
                    "kba_id": kba.get("SitRecID"),
                }
                stats = {**kba_properties, **kba_area_stats}

                return ee.Feature(None, {"stats": stats}).set(stats)

            def pa_stats(pa):
                ls_pa = ee.Feature(pa.geometry().intersection(feat.geometry()))
                pa_area_stats = habitat_areas(ls_pa)
                pa_properties = {
                    "pa_name": pa.get("NAME"),
                    "pa_id": pa.get("WDPAID"),
                }
                stats = {**pa_properties, **pa_area_stats}

                return ee.Feature(None, {"stats": stats}).set(stats)

            ecoregions = self.ecoregions.filterBounds(feat.geometry()).map(
                ecoregion_stats
            )
            ecoregion_area_stats = ecoregions.aggregate_array("stats")

            kbas = self.kbas.filterBounds(feat.geometry()).map(kba_stats)
            kba_area_stats = kbas.aggregate_array("stats")

            pas = self.pas.filterBounds(feat.geometry()).map(pa_stats)
            pa_area_stats = pas.aggregate_array("stats")

            return (
                ls_state_stats.set(ee.Dictionary({"ecoregions": ecoregion_area_stats}))
                .set(ee.Dictionary({"kbas": kba_area_stats}))
                .set(ee.Dictionary({"pas": pa_area_stats}))
                .set(
                    {
                        "kba_total_area": kbas.aggregate_sum("total_area"),
                        "kba_indigenous_range_area": kbas.aggregate_sum(
                            "indigenous_range_area"
                        ),
                        "kba_str_hab_area": kbas.aggregate_sum("str_hab_area"),
                        "kba_eff_pot_hab_area": kbas.aggregate_sum("eff_pot_hab_area"),
                        "kba_connected_eff_pot_hab_area": kbas.aggregate_sum(
                            "connected_eff_pot_hab_area"
                        ),
                        "kba_occupied_eff_pot_hab_area": kbas.aggregate_sum(
                            "occupied_eff_pot_hab_area"
                        ),
                        "pa_total_area": pas.aggregate_sum("total_area"),
                        "pa_indigenous_range_area": pas.aggregate_sum(
                            "indigenous_range_area"
                        ),
                        "pa_str_hab_area": pas.aggregate_sum("str_hab_area"),
                        "pa_eff_pot_hab_area": pas.aggregate_sum("eff_pot_hab_area"),
                        "pa_connected_eff_pot_hab_area": pas.aggregate_sum(
                            "connected_eff_pot_hab_area"
                        ),
                        "pa_occupied_eff_pot_hab_area": pas.aggregate_sum(
                            "occupied_eff_pot_hab_area"
                        ),
                    }
                )
            )

        area_stats = landscapes.map(feature_area_stats)
        self.table2storage(area_stats, self.DEFAULT_BUCKET, blob)

    def export_landscape_geometries(self, types, feature_collections):
        for i in range(len(types)):
            self.calc_landscape_geometries(
                f"scl_{self.SPECIES}", feature_collections[i], types[i], True
            )
            self.calc_landscape_geometries(
                f"scl_{self.RESTORATION}", feature_collections[i], types[i], True
            )
            self.calc_landscape_geometries(
                f"scl_{self.SURVEY}", feature_collections[i], types[i], True
            )
            self.calc_landscape_geometries(
                f"scl_{self.SPECIES}_{self.FRAGMENT}",
                feature_collections[i],
                types[i],
                True,
            )
            self.calc_landscape_geometries(
                f"scl_{self.RESTORATION}_{self.FRAGMENT}",
                feature_collections[i],
                types[i],
                True,
            )
            self.calc_landscape_geometries(
                f"scl_{self.SURVEY}_{self.FRAGMENT}",
                feature_collections[i],
                types[i],
                True,
            )

    def calc_state_areas(self):
        bucket = self.gcsclient.get_bucket(self.DEFAULT_BUCKET)
        blob = f"ls_stats/{self.species}/{self.scenario}/{self.taskdate}/scl_states"

        def _round(feat):
            geom = feat.geometry()
            ind_range = feat.get("indigenous_range_area")
            eph = feat.get("eff_pot_hab_area")
            ceph = feat.get("connected_eff_pot_hab_area")
            oeph = feat.get("occupied_eff_pot_hab_area")
            strh = feat.get("str_hab_area")
            return feat.set(
                {
                    "indigenous_range_area": ee.Number(ind_range).round(),
                    "eff_pot_hab_area": ee.Number(eph).round(),
                    "connected_eff_pot_hab_area": ee.Number(ceph).round(),
                    "occupied_eff_pot_hab_area": ee.Number(oeph).round(),
                    "str_hab_area": ee.Number(strh).round(),
                }
            )

        areas_fc = self.area_image.reduceRegions(
            collection=self.states.filterBounds(self.historical_range_fc),
            reducer=ee.Reducer.sum(),
            scale=self.scale,
            crs=self.crs,
        ).map(_round)
        self.table2storage(areas_fc, self.DEFAULT_BUCKET, blob)

    def calc_country_historical_range(self):

        bucket = self.gcsclient.get_bucket(self.DEFAULT_BUCKET)
        blob = f"ls_stats/{self.species}/country_historical_range"
        if bucket.get_blob(f"{blob}.geojson"):
            print("Skipping country / historical range calculation (already exists)")
            return

        historical_geom = self.historical_range_fc.geometry()

        def get_country_historical_range(country):
            country_hr = country.geometry().intersection(
                historical_geom, self.error_margin
            )
            area = ee.Image.pixelArea().divide(1000000).updateMask(self.watermask)
            country_hr_area = self.rounded_habitat_area(country_hr, area, "area", 30)
            props = {
                "country": country.get("iso2"),
                "area": country_hr_area,
            }

            return ee.Feature(country_hr, props)

        country_hrs = self.countries.map(get_country_historical_range)
        self.table2storage(country_hrs, self.DEFAULT_BUCKET, blob)

    def calc(self):
        area = ee.Image.pixelArea().divide(1000000).updateMask(self.watermask)
        str_hab_area = area.updateMask(self.structural_habitat)
        range_area = area.updateMask(self.historical_range)
        self.area_image = area.addBands(
            [
                range_area,
                str_hab_area,
                self.scl_image.select(
                    ["eff_pot_hab_area", "connected_eff_pot_hab_area"]
                ),
                self.occupied_habitat_image,
            ]
        ).rename(
            [
                "total_area",
                "indigenous_range_area",
                "str_hab_area",
                "eff_pot_hab_area",
                "connected_eff_pot_hab_area",
                "occupied_eff_pot_hab_area",
            ]
        )

        self.calc_state_areas()
        self.calc_country_historical_range()
        self.export_landscape_geometries(
            ["landscape", "state", "country"], [None, self.states, self.countries]
        )
        self.calc_landscape_stats(f"{self.SPECIES}")
        self.calc_landscape_stats(f"{self.SURVEY}")
        self.calc_landscape_stats(f"{self.RESTORATION}")
        self.calc_landscape_stats(f"{self.SPECIES}_{self.FRAGMENT}")
        self.calc_landscape_stats(f"{self.SURVEY}_{self.FRAGMENT}")
        self.calc_landscape_stats(f"{self.RESTORATION}_{self.FRAGMENT}")

    def check_inputs(self):
        super().check_inputs()
        # add any task-specific checks here, and set self.status = FAILED if any fail


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--taskdate")
    parser.add_argument("-s", "--species")
    parser.add_argument("--scenario")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="overwrite existing outputs instead of incrementing",
    )
    options = parser.parse_args()
    sclstats_task = SCLStats(**vars(options))
    sclstats_task.run()
