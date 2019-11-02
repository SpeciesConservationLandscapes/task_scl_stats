import ee
from task_base import SCLTask


class SCLStats(SCLTask):
    ee_rootdir = "projects/SCL/v1"
    ee_aoi = 'sumatra_poc_aoi'
    # if input lives in ee, it should have an "ee_path" pointing to an ImageCollection/FeatureCollection
    inputs = {
        # "scl": {
        #     "ee_type": EETask.IMAGECOLLECTION,
        #     "ee_path": "{}/{}/scl".format(ee_rootdir, SCLTask.species),
        #     "maxage": 1/365  # years
        # }
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def calc(self):
        # Given geographies dirs in ee, table relating geographies to countries, protected areas, and habitat types,
        # calculate zonal stats broken down by all
        pass

    def check_inputs(self):
        super().check_inputs()
        # add any task-specific checks here, and set self.status = FAILED if any fail


if __name__ == "__main__":
    sclstats_task = SCLStats()
    sclstats_task.run()
