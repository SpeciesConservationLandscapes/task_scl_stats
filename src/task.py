import argparse
import ee
from task_base import SCLTask


class SCLStats(SCLTask):
    ee_rootdir = "projects/SCL/v1"
    inputs = {
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
    }

    def rounded_area(self, geom):
        stats = ee.Image.pixelArea().reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            scale=30,  # deliberately not using self.scale for greater area precision
            maxPixels=self.ee_max_pixels,
        )
        return (
            ee.Number(stats.get("area"))
            .multiply(0.000001)
            .multiply(10)
            .round()
            .multiply(0.1)
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.scenario != self.CANONICAL:
            raise NotImplementedError(
                "Generating statistics for non-canonical species landscape calculations not implemented until "
                "there is a consumer or use case defined for it, as SCL API ingestion is for canonical."
            )
        self.error_margin = ee.ErrorMargin(1)
        self.kbas = ee.FeatureCollection(self.inputs["kbas"]["ee_path"])

    def calc_landscapes(self, landscape_key):
        landscapes, landscapes_date = self.get_most_recent_featurecollection(
            self.inputs[landscape_key]["ee_path"]
        )

        if landscapes is None:
            return

        def get_ls_countries_biomes_pas(ls):
            ls_total_area = self.rounded_area(ls.geometry())

            def get_ls_countries_biomes(country):
                ls_country = ls.geometry().intersection(
                    country.geometry(), self.error_margin
                )
                ls_country_area = self.rounded_area(ls_country)
                ls_country_biomes = self.ecoregions.filterBounds(ls_country)

                def get_ls_countries_biome_numbers(biome_num):
                    biome_num = ee.Number.parse(biome_num).int()
                    biome = ls_country_biomes.filter(
                        ee.Filter.eq("BIOME_NUM", biome_num)
                    )
                    biome_geometry = (
                        biome.union()
                        .geometry()
                        .intersection(ls_country, self.error_margin)
                    )
                    biome_name = ee.Feature(biome.first()).get("BIOME_NAME")

                    ls_country_biome_pas = self.pas.filterBounds(biome_geometry)
                    ls_country_biome_protected = (
                        ls_country_biome_pas.union()
                        .geometry()
                        .intersection(biome_geometry, self.error_margin)
                    )
                    ls_country_biome_protected_area = self.rounded_area(
                        ls_country_biome_protected
                    )
                    ls_country_biome_unprotected_area = self.rounded_area(
                        biome_geometry.difference(ls_country_biome_protected)
                    )

                    ls_country_biome_kbas = self.kbas.filterBounds(biome_geometry)
                    ls_country_biome_kbageom = (
                        ls_country_biome_kbas.union()
                        .geometry()
                        .intersection(biome_geometry, self.error_margin)
                    )
                    ls_country_biome_kba_area = self.rounded_area(
                        ls_country_biome_kbageom
                    )
                    ls_country_biome_nonkba_area = self.rounded_area(
                        biome_geometry.difference(ls_country_biome_kbageom)
                    )

                    def get_ls_country_biome_pas(pa_id):
                        ls_country_biome_pa_id = ee.Number.parse(pa_id).int()
                        pa = ls_country_biome_pas.filter(
                            ee.Filter.eq("WDPAID", ls_country_biome_pa_id)
                        )
                        ls_country_biome_pa_name = ee.Feature(pa.first()).get("NAME")
                        ls_country_biome_pa_area = self.rounded_area(
                            pa.geometry().intersection(
                                biome_geometry, self.error_margin
                            )
                        )

                        return ee.Dictionary(
                            {
                                "paname": ls_country_biome_pa_name,
                                "paid": ls_country_biome_pa_id,
                                "paarea": ls_country_biome_pa_area,
                            }
                        )

                    ls_country_biome_pa_areas = ee.List(
                        ee.Dictionary(
                            ls_country_biome_pas.aggregate_histogram("WDPAID")
                        ).keys()
                    ).map(get_ls_country_biome_pas)

                    ls_country_biome_kbas = self.kbas.filterBounds(biome_geometry)

                    def get_ls_country_biome_kbas(kba_id):
                        ls_country_biome_kba_id = ee.Number.parse(kba_id).int()
                        kba = ls_country_biome_kbas.filter(
                            ee.Filter.eq("SitRecID", ls_country_biome_kba_id)
                        )
                        ls_country_biome_kba_name = ee.Feature(kba.first()).get(
                            "IntName"
                        )
                        ls_country_biome_kba_area = self.rounded_area(
                            kba.geometry().intersection(
                                biome_geometry, self.error_margin
                            )
                        )

                        return ee.Dictionary(
                            {
                                "kbaname": ls_country_biome_kba_name,
                                "kbaid": ls_country_biome_kba_id,
                                "kbaarea": ls_country_biome_kba_area,
                            }
                        )

                    ls_country_biome_kba_areas = ee.List(
                        ee.Dictionary(
                            ls_country_biome_kbas.aggregate_histogram("SitRecID")
                        ).keys()
                    ).map(get_ls_country_biome_kbas)

                    return ee.Dictionary(
                        {
                            "biome": {"biomeid": biome_num, "biomename": biome_name},
                            "pas": ls_country_biome_pa_areas,
                            "kbas": ls_country_biome_kba_areas,
                            "protected": ls_country_biome_protected_area,
                            "unprotected": ls_country_biome_unprotected_area,
                            "kba_area": ls_country_biome_kba_area,
                            "nonkba_area": ls_country_biome_nonkba_area,
                        }
                    )

                ls_country_biome_numbers = ee.List(
                    ee.Dictionary(
                        ls_country_biomes.aggregate_histogram("BIOME_NUM")
                    ).keys()
                ).map(get_ls_countries_biome_numbers)

                props = {
                    "lsid": ls.get("dissolved_poly_id"),
                    "lscountry": country.get("ISO"),
                    "ls_total_area": ls_total_area,
                    "lscountry_area": ls_country_area,
                    "areas": ls_country_biome_numbers,
                }
                if landscape_key == "scl_species":
                    _name = ls.get("name")
                    _class = ls.get("class")
                    if _name is not None:
                        props["lsname"] = _name
                    if _class is not None:
                        props["lsclass"] = _class

                return ee.Feature(ls_country, props)

            return self.countries.filterBounds(ls.geometry()).map(
                get_ls_countries_biomes
            )

        ls_countries_biomes_pas = landscapes.map(get_ls_countries_biomes_pas).flatten()

        blob = (
            f"ls_stats/{self.species}/{self.scenario}/{self.taskdate}/{landscape_key}"
        )
        self.table2storage(ls_countries_biomes_pas, self.DEFAULT_BUCKET, blob)

    def calc_country_historical_range(self):
        bucket = self.gcsclient.get_bucket(self.DEFAULT_BUCKET)
        blob = f"ls_stats/{self.species}/country_historical_range"
        if bucket.get_blob(f"{blob}.geojson"):
            print("Skipping country / historical range calculation (already exists)")
            return

        historical_geom = self.historical_range_fc.first().geometry()

        def get_country_historical_range(country):
            country_hr = country.geometry().intersection(
                historical_geom, self.error_margin
            )
            country_hr_area = self.rounded_area(country_hr)
            props = {
                "country": country.get("ISO"),
                "area": country_hr_area,
            }

            return ee.Feature(country_hr, props)

        country_hrs = self.countries.filterBounds(historical_geom).map(
            get_country_historical_range
        )
        self.table2storage(country_hrs, self.DEFAULT_BUCKET, blob)

    def calc(self):
        self.calc_country_historical_range()
        self.calc_landscapes(f"scl_{SCLTask.SPECIES}")
        self.calc_landscapes(f"scl_{SCLTask.RESTORATION}")
        self.calc_landscapes(f"scl_{SCLTask.SURVEY}")
        self.calc_landscapes(f"scl_{SCLTask.SPECIES}_{SCLTask.FRAGMENT}")
        self.calc_landscapes(f"scl_{SCLTask.RESTORATION}_{SCLTask.FRAGMENT}")
        self.calc_landscapes(f"scl_{SCLTask.SURVEY}_{SCLTask.FRAGMENT}")

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
