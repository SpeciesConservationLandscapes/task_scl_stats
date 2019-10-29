import ee
import time
from task_base import SCLTask


class SCLStats(SCLTask):
    ee_rootdir = "projects/SCL/v1"
    ee_aoi = 'sumatra_poc_aoi'
    # if input lives in ee, it should have an "ee_path" pointing to an ImageCollection/FeatureCollection
    inputs = {
        # "scl": {
        #     "ee_path": "{}/{}/scl".format(ee_rootdir, species),
        #     "maxage": 1/365  # years
        # }
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run_calc(self):
        # Given geographies dirs in ee, table relating geographies to countries, protected areas, and habitat types,
        # calculate zonal stats broken down by all
        pass

    def check_inputs(self):
        super().check_inputs()
        # add any task-specific checks here, and set self.status = SKIPPED if any fail

    def run(self):
        super().run()
        if self.status == self.RUNNING:
            self.run_calc()
            while self.get_unfinished_ee_tasks():
                time.sleep(30)
            self.status = self.COMPLETE

        print('status: {}'.format(self.status))


sclstats_task = SCLStats()
sclstats_task.run()
