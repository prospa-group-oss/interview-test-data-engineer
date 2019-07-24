import luigi

import datetime as dt
import dwh_loader,tp_loader,report_generator

class file2Database(luigi.Task):
    day = luigi.Parameter()
    def output(self):
        return luigi.LocalTarget("_COMPETED_LOADING_{}".format(self.day))

    def run(self):
        tp_loader.tp_load()
        with open("_COMPETED_LOADING_{}".format(self.day),"w") as out:
            out.write("done")

class dwhLoader(luigi.Task):
    day = luigi.Parameter()
    def output(self):
        return luigi.LocalTarget("_DWH_LOAD_DONE_{}".format(self.day))

    def requires(self):
        return file2Database(day=self.day)

    def run(self):
        dwh_loader.dwh_loader()
        with open("_DWH_LOAD_DONE_{}".format(self.day),"w") as out:
            out.write("done")

class reportGeneration(luigi.Task):
    day = luigi.Parameter(default=dt.datetime.now().day)
    def output(self):
        return luigi.LocalTarget("_REPORT_DONE")

    def requires(self):
        return dwhLoader(day=self.day)

    def run(self):
        report_generator.report_gen()