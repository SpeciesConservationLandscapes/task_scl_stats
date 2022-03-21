Species Conservation Landscapes areal statistics calculation task
-----------------------------------------------------------------

Task for calculating zonal statistics for each of the types assigned to species habitat by the 
`scl_classification` task by country, protected status, biome, and Key Biodiversity Area (KBA). 
Output intersected polygons with statistics are exported to GeoJSON files in Google Storage, for ingestion
by the SCL API.

## Usage

*All parameters may be specified in the environment as well as the command line.*

```
/app # python task.py --help
usage: task.py [-h] [-d TASKDATE] [-s SPECIES] [--scenario SCENARIO] [--overwrite]

optional arguments:
  -h, --help            show this help message and exit
  -d TASKDATE, --taskdate TASKDATE
  -s SPECIES, --species SPECIES
  --scenario SCENARIO
  --overwrite           overwrite existing outputs instead of incrementing
```

### License
Copyright (C) 2022 Wildlife Conservation Society
The files in this repository  are part of the task framework for calculating 
Human Impact Index and Species Conservation Landscapes (https://github.com/SpeciesConservationLandscapes) 
and are released under the GPL license:
https://www.gnu.org/licenses/#GPL
See [LICENSE](./LICENSE) for details.
