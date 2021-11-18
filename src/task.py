import argparse
import ee
from datetime import datetime, timezone
from task_base import SCLTask


class SCLStats(SCLTask):
    ee_rootdir = "projects/SCL/v1"
    inputs = {
        "historical_range": {
            "ee_type": SCLTask.IMAGE,
            "ee_path": "historical_range_path",
            "static": True,
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
        "scl_fragment": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": f"scl_path_{SCLTask.FRAGMENT}",
            "maxage": 1 / 365,
        },
        "countries": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": "USDOS/LSIB/2013",
            "maxage": 10,
        },
        "ecoregions": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": "RESOLVE/ECOREGIONS/2017",
            "maxage": 5,
        },
        "pas": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": "WCMC/WDPA/current/polygons",
            "maxage": 1,
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
        self.error_margin = ee.ErrorMargin(1)
        self.historical_range_fc = ee.FeatureCollection(
            self.inputs["historical_range"]["ee_path"]
        )
        self.countries = ee.FeatureCollection(self.inputs["countries"]["ee_path"])
        self.ecoregions = ee.FeatureCollection(self.inputs["ecoregions"]["ee_path"])
        taskyear = ee.Date(self.taskdate.strftime(self.DATE_FORMAT)).get("year")
        self.pas = (
            ee.FeatureCollection(self.inputs["pas"]["ee_path"])
            .filterBounds(self.historical_range_fc.geometry())
            .filter(ee.Filter.neq("STATUS", "Proposed"))
            .filter(ee.Filter.lte("STATUS_YR", taskyear))
        )

    def historical_range_path(self):
        return f"{self.speciesdir}/historical_range"

    def calc_landscapes(self, landscape_key):
        landscapes, landscapes_date = self.get_most_recent_featurecollection(
            self.inputs[landscape_key]["ee_path"]
        )

        if landscapes is None:
            return

        def get_ls_countries_biomes_pas(ls):
            # TODO: add unique id from ls when we have it
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
                    ls_country_biome_unprotected = ls_country.difference(
                        ls_country_biome_protected
                    )
                    ls_country_biome_protected_area = self.rounded_area(
                        ls_country_biome_protected
                    )
                    ls_country_biome_unprotected_area = self.rounded_area(
                        ls_country_biome_unprotected
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

                    ls_country_biome_pas = ee.List(
                        ee.Dictionary(
                            ls_country_biome_pas.aggregate_histogram("WDPAID")
                        ).keys()
                    ).map(get_ls_country_biome_pas)

                    return ee.Dictionary(
                        {
                            "biome": {"biomeid": biome_num, "biomename": biome_name},
                            "pas": ls_country_biome_pas,
                            "protected": ls_country_biome_protected_area,
                            "unprotected": ls_country_biome_unprotected_area,
                        }
                    )

                ls_country_biome_numbers = ee.List(
                    ee.Dictionary(
                        ls_country_biomes.aggregate_histogram("BIOME_NUM")
                    ).keys()
                ).map(get_ls_countries_biome_numbers)

                props = {
                    "lscountry": country.get("iso_alpha2"),
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
        self.table2storage(ls_countries_biomes_pas, "scl-pipeline", blob)

    def calc(self):
        self.calc_landscapes(f"scl_{SCLTask.SPECIES}")
        self.calc_landscapes(f"scl_{SCLTask.RESTORATION}")
        self.calc_landscapes(f"scl_{SCLTask.SURVEY}")
        self.calc_landscapes(f"scl_{SCLTask.FRAGMENT}")

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
