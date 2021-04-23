import argparse
import ee
from datetime import datetime, timezone
from task_base import SCLTask


# USDOS/LSIB/2017 apparently dropped iso support
USDOS_NAME_FIELD = "COUNTRY_NA"
ISO_FIELD = "iso_alpha2"


class SCLStats(SCLTask):
    ee_rootdir = "projects/SCL/v1"
    inputs = {
        "scl_species": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": f"scl_path_{SCLTask.SPECIES}",
            "maxage": 1 / 365,  # years
        },
        # "scl_restoration": {
        #     "ee_type": SCLTask.FEATURECOLLECTION,
        #     "ee_path": f"scl_path_{SCLTask.RESTORATION}",
        #     "maxage": 1 / 365,
        # },
        "scl_survey": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": f"scl_path_{SCLTask.SURVEY}",
            "maxage": 1 / 365,
        },
        # "scl_fragment": {
        #     "ee_type": SCLTask.FEATURECOLLECTION,
        #     "ee_path": f"scl_path_{SCLTask.FRAGMENT}",
        #     "maxage": 1 / 365,
        # },
        "countries": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": "USDOS/LSIB/2017",  # 2013 also exists
            "static": True,
        },
        "leuser": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": "projects/SCL/v1/Panthera_tigris/geographies/Sumatra/leuser",
            "static": True,
        },
        "ecoregions": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": "RESOLVE/ECOREGIONS/2017",
            "static": True,
        },
        "pas": {
            "ee_type": SCLTask.FEATURECOLLECTION,
            "ee_path": "WCMC/WDPA/current/polygons",
            "maxage": 1,
            # TODO:
            # WCMC updates the WDPA on a monthly basis. The most recent version is always available as
            # WCMC/WDPA/current/polygons and WCMC/WDPA/current/points. Historical versions, starting with July 2017,
            # are available in the format WCMC/WDPA/YYYYMM/polygons and WCMC/WDPA/YYYYMM/points.
        },
    }

    # temporary legacy for Leuser
    def _scl_path(self, scltype):
        if scltype is None or scltype not in self.LANDSCAPE_TYPES:
            raise TypeError("Missing or incorrect scltype for setting scl path")
        return f"projects/SCL/v1/Panthera_tigris/geographies/Sumatra/scl_poly/{self.taskdate}/scl_{scltype}"

    # matches (ISO 3166-1) django_countries used in scl-api
    ISO_COUNTRY_DICT = {
        "Afghanistan": "AF",
        "Åland Islands": "AX",
        "Albania": "AL",
        "Algeria": "DZ",
        "American Samoa": "AS",
        "Andorra": "AD",
        "Angola": "AO",
        "Anguilla": "AI",
        "Antarctica": "AQ",
        "Antigua and Barbuda": "AG",
        "Argentina": "AR",
        "Armenia": "AM",
        "Aruba": "AW",
        "Australia": "AU",
        "Austria": "AT",
        "Azerbaijan": "AZ",
        "Bahamas": "BS",
        "Bahrain": "BH",
        "Bangladesh": "BD",
        "Barbados": "BB",
        "Belarus": "BY",
        "Belgium": "BE",
        "Belize": "BZ",
        "Benin": "BJ",
        "Bermuda": "BM",
        "Bhutan": "BT",
        "Bolivia": "BO",
        "Bonaire, Sint Eustatius and Saba": "BQ",
        "Bosnia and Herzegovina": "BA",
        "Botswana": "BW",
        "Bouvet Island": "BV",
        "Brazil": "BR",
        "British Indian Ocean Territory": "IO",
        "Brunei": "BN",
        "Bulgaria": "BG",
        "Burkina Faso": "BF",
        "Burundi": "BI",
        "Cabo Verde": "CV",
        "Cambodia": "KH",
        "Cameroon": "CM",
        "Canada": "CA",
        "Cayman Islands": "KY",
        "Central African Republic": "CF",
        "Chad": "TD",
        "Chile": "CL",
        "China": "CN",
        "Christmas Island": "CX",
        "Cocos (Keeling) Islands": "CC",
        "Colombia": "CO",
        "Comoros": "KM",
        "Congo": "CG",
        "Congo (the Democratic Republic of the)": "CD",
        "Cook Islands": "CK",
        "Costa Rica": "CR",
        "Côte d'Ivoire": "CI",
        "Croatia": "HR",
        "Cuba": "CU",
        "Curaçao": "CW",
        "Cyprus": "CY",
        "Czechia": "CZ",
        "Denmark": "DK",
        "Djibouti": "DJ",
        "Dominica": "DM",
        "Dominican Republic": "DO",
        "Ecuador": "EC",
        "Egypt": "EG",
        "El Salvador": "SV",
        "Equatorial Guinea": "GQ",
        "Eritrea": "ER",
        "Estonia": "EE",
        "Eswatini": "SZ",
        "Ethiopia": "ET",
        "Falkland Islands (Malvinas)": "FK",
        "Faroe Islands": "FO",
        "Fiji": "FJ",
        "Finland": "FI",
        "France": "FR",
        "French Guiana": "GF",
        "French Polynesia": "PF",
        "French Southern Territories": "TF",
        "Gabon": "GA",
        "Gambia": "GM",
        "Georgia": "GE",
        "Germany": "DE",
        "Ghana": "GH",
        "Gibraltar": "GI",
        "Greece": "GR",
        "Greenland": "GL",
        "Grenada": "GD",
        "Guadeloupe": "GP",
        "Guam": "GU",
        "Guatemala": "GT",
        "Guernsey": "GG",
        "Guinea": "GN",
        "Guinea-Bissau": "GW",
        "Guyana": "GY",
        "Haiti": "HT",
        "Heard Island and McDonald Islands": "HM",
        "Holy See": "VA",
        "Honduras": "HN",
        "Hong Kong": "HK",
        "Hungary": "HU",
        "Iceland": "IS",
        "India": "IN",
        "Indonesia": "ID",
        "Iran": "IR",
        "Iraq": "IQ",
        "Ireland": "IE",
        "Isle of Man": "IM",
        "Israel": "IL",
        "Italy": "IT",
        "Jamaica": "JM",
        "Japan": "JP",
        "Jersey": "JE",
        "Jordan": "JO",
        "Kazakhstan": "KZ",
        "Kenya": "KE",
        "Kiribati": "KI",
        "Kuwait": "KW",
        "Kyrgyzstan": "KG",
        "Laos": "LA",
        "Latvia": "LV",
        "Lebanon": "LB",
        "Lesotho": "LS",
        "Leuser": "LE",
        "Liberia": "LR",
        "Libya": "LY",
        "Liechtenstein": "LI",
        "Lithuania": "LT",
        "Luxembourg": "LU",
        "Macao": "MO",
        "Madagascar": "MG",
        "Malawi": "MW",
        "Malaysia": "MY",
        "Maldives": "MV",
        "Mali": "ML",
        "Malta": "MT",
        "Marshall Islands": "MH",
        "Martinique": "MQ",
        "Mauritania": "MR",
        "Mauritius": "MU",
        "Mayotte": "YT",
        "Mexico": "MX",
        "Micronesia (Federated States of)": "FM",
        "Moldova": "MD",
        "Monaco": "MC",
        "Mongolia": "MN",
        "Montenegro": "ME",
        "Montserrat": "MS",
        "Morocco": "MA",
        "Mozambique": "MZ",
        "Myanmar": "MM",
        "Namibia": "NA",
        "Nauru": "NR",
        "Nepal": "NP",
        "Netherlands": "NL",
        "New Caledonia": "NC",
        "New Zealand": "NZ",
        "Nicaragua": "NI",
        "Niger": "NE",
        "Nigeria": "NG",
        "Niue": "NU",
        "Norfolk Island": "NF",
        "North Korea": "KP",
        "North Macedonia": "MK",
        "Northern Mariana Islands": "MP",
        "Norway": "NO",
        "Oman": "OM",
        "Pakistan": "PK",
        "Palau": "PW",
        "Palestine, State of": "PS",
        "Panama": "PA",
        "Papua New Guinea": "PG",
        "Paraguay": "PY",
        "Peru": "PE",
        "Philippines": "PH",
        "Pitcairn": "PN",
        "Poland": "PL",
        "Portugal": "PT",
        "Puerto Rico": "PR",
        "Qatar": "QA",
        "Réunion": "RE",
        "Romania": "RO",
        "Russia": "RU",
        "Rwanda": "RW",
        "Saint Barthélemy": "BL",
        "Saint Helena, Ascension and Tristan da Cunha": "SH",
        "Saint Kitts and Nevis": "KN",
        "Saint Lucia": "LC",
        "Saint Martin (French part)": "MF",
        "Saint Pierre and Miquelon": "PM",
        "Saint Vincent and the Grenadines": "VC",
        "Samoa": "WS",
        "San Marino": "SM",
        "Sao Tome and Principe": "ST",
        "Saudi Arabia": "SA",
        "Senegal": "SN",
        "Serbia": "RS",
        "Seychelles": "SC",
        "Sierra Leone": "SL",
        "Singapore": "SG",
        "Sint Maarten (Dutch part)": "SX",
        "Slovakia": "SK",
        "Slovenia": "SI",
        "Solomon Islands": "SB",
        "Somalia": "SO",
        "South Africa": "ZA",
        "South Georgia and the South Sandwich Islands": "GS",
        "South Korea": "KR",
        "South Sudan": "SS",
        "Spain": "ES",
        "Sri Lanka": "LK",
        "Sudan": "SD",
        "Suriname": "SR",
        "Svalbard and Jan Mayen": "SJ",
        "Sweden": "SE",
        "Switzerland": "CH",
        "Syria": "SY",
        "Taiwan": "TW",
        "Tajikistan": "TJ",
        "Tanzania": "TZ",
        "Thailand": "TH",
        "Timor-Leste": "TL",
        "Togo": "TG",
        "Tokelau": "TK",
        "Tonga": "TO",
        "Trinidad and Tobago": "TT",
        "Tunisia": "TN",
        "Turkey": "TR",
        "Turkmenistan": "TM",
        "Turks and Caicos Islands": "TC",
        "Tuvalu": "TV",
        "Uganda": "UG",
        "Ukraine": "UA",
        "United Arab Emirates": "AE",
        "United Kingdom": "GB",
        "United States Minor Outlying Islands": "UM",
        "United States of America": "US",
        "Uruguay": "UY",
        "Uzbekistan": "UZ",
        "Vanuatu": "VU",
        "Venezuela": "VE",
        "Vietnam": "VN",
        "Virgin Islands (British)": "VG",
        "Virgin Islands (U.S.)": "VI",
        "Wallis and Futuna": "WF",
        "Western Sahara": "EH",
        "Yemen": "YE",
        "Zambia": "ZM",
        "Zimbabwe": "ZW"
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

    def get_iso(self, feat):
        name = feat.get(USDOS_NAME_FIELD)
        iso = ee.Dictionary(self.ISO_COUNTRY_DICT).get(name, "")
        return feat.set(ISO_FIELD, iso)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.error_margin = ee.ErrorMargin(1)
        self.countries = ee.FeatureCollection(self.inputs["countries"]["ee_path"]).map(self.get_iso)\
            .merge(self.inputs["leuser"]["ee_path"])
        self.ecoregions = ee.FeatureCollection(self.inputs["ecoregions"]["ee_path"])
        self.pas = ee.FeatureCollection(self.inputs["pas"]["ee_path"])

    def calc_landscapes(self, landscape_key):
        # landscapes, landscapes_date = self.get_most_recent_featurecollection(
        #     self.inputs[landscape_key]["ee_path"]
        # )
        landscapes = ee.FeatureCollection(self.inputs[landscape_key]["ee_path"])

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
                    "lscountry": country.get(ISO_FIELD),
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
        self.export_fc_cloudstorage(ls_countries_biomes_pas, "scl-pipeline", blob)

    def calc(self):
        self.calc_landscapes(f"scl_{SCLTask.SPECIES}")
        # self.calc_landscapes(f"scl_{SCLTask.RESTORATION}")
        self.calc_landscapes(f"scl_{SCLTask.SURVEY}")
        # self.calc_landscapes(f"scl_{SCLTask.FRAGMENT}")

    def check_inputs(self):
        super().check_inputs()
        # add any task-specific checks here, and set self.status = FAILED if any fail


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--taskdate", default=datetime.now(timezone.utc).date())
    parser.add_argument("-s", "--species", default="Panthera_tigris")
    parser.add_argument("--scenario", default=SCLTask.CANONICAL)
    options = parser.parse_args()
    sclstats_task = SCLStats(**vars(options))
    sclstats_task.run()
