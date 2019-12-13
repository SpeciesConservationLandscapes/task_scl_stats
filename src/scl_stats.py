import argparse
import ee
from datetime import datetime, timezone
from task_base import SCLTask


class SCLStats(SCLTask):
    ee_rootdir = "projects/SCL/v1"
    inputs = {
        "scl_species": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": "scl_path_species",
            "maxage": 1 / 365,  # years
        },
        "scl_restoration": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": "scl_path_restoration",
            "maxage": 1 / 365,
        },
        "scl_survey": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": "scl_path_survey",
            "maxage": 1 / 365,
        },
        "scl_fragment": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": "scl_path_fragment",
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

    def _scl_path(self, scltype):
        if scltype is None or scltype not in self.inputs:
            raise TypeError("Missing or incorrect scltype for setting scl path")
        return "{}/{}/scl_poly/{}/scl-species".format(
            self.ee_rootdir, self.species, self.taskdate
        )

    def scl_path_species(self):
        return self._scl_path("scl_species")

    def scl_path_restoration(self):
        return self._scl_path("scl_restoration")

    def scl_path_survey(self):
        return self._scl_path("scl_survey")

    def scl_path_fragment(self):
        return self._scl_path("scl_fragment")

    def rounded_area(self, geom):
        return (
            geom.area(self.error_margin, self.area_proj)
                .multiply(0.000001)
                .multiply(10)
                .round()
                .multiply(0.01)
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.error_margin = ee.ErrorMargin(1)
        self.area_proj = "EPSG:5070"
        self.countries = ee.FeatureCollection(self.inputs["countries"]["ee_path"])
        self.ecoregions = ee.FeatureCollection(self.inputs["ecoregions"]["ee_path"])
        self.pas = ee.FeatureCollection(self.inputs["pas"]["ee_path"])

    def calc_landscapes(self, landscape_key):
        landscapes = ee.FeatureCollection(self.inputs[landscape_key]["ee_path"])

        def get_ls_countries_biomes_pas(ls):
            ls_total_area = self.rounded_area(ls)

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
                    "scl_country": country.get("iso_alpha2"),
                    "scl_total_area": ls_total_area,
                    "scl_country_area": ls_country_area,
                    "areas": ls_country_biome_numbers,
                }
                if landscape_key == "scl_species":
                    props["scl_name"] = ls.get("name")
                    props["scl_class"] = ls.get("class")

                return ee.Feature(ls_country, props)

            return self.countries.filterBounds(ls.geometry()).map(
                get_ls_countries_biomes
            )

        ls_countries_biomes_pas = landscapes.map(get_ls_countries_biomes_pas).flatten()

        blob = "ls_stats/{}/{}/{}".format(self.species, self.taskdate, landscape_key)
        self.export_fc_cloudstorage(ls_countries_biomes_pas, "scl-pipeline", blob)

    def calc(self):
        self.calc_landscapes("scl_species")
        self.calc_landscapes("scl_restoration")
        self.calc_landscapes("scl_survey")
        self.calc_landscapes("scl_fragment")

    def check_inputs(self):
        super().check_inputs()
        # add any task-specific checks here, and set self.status = FAILED if any fail


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--taskdate", default=datetime.now(timezone.utc).date())
    parser.add_argument("-s", "--species", default="Panthera_tigris")
    options = parser.parse_args()
    sclstats_task = SCLStats(**vars(options))
    sclstats_task.run()
