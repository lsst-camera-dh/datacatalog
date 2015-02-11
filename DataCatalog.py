"""
Use Brian's python client to query the SRS datacatalog.
"""

import os
import subprocess
import datacat

remote_hosts = {'SLAC' : 'centaurusa.slac.stanford.edu'}

class DatasetList(list):
    def __init__(self, my_list, datacat_obj):
        super(DatasetList, self).__init__(my_list)
        self.folder = datacat_obj.folder
        self.login = datacat_obj.remote_login
    def filenames(self):
        return [str(x.name) for x in self]
    def download(self, site='SLAC', rootpath='.', nfiles=None, dryrun=True,
                 clobber=False):
        user_host = '@'.join((self.login, remote_hosts[site]))
        if nfiles is not None:
            print "Downloading the first %i files:\n" % nfiles
        if dryrun:
            print "Dry run. The following commands would be executed:\n"
        my_datasets = []
        for dataset in datasets[:nfiles]:
            for location in dataset.locations:
                if location.site == site:
                    my_datasets.append(dataset)
        for dataset in my_datasets:
            output = os.path.join(rootpath, 
                                  dataset.path[len(self.folder)+1:])
            outdir = os.path.split(output)[0]
            if not os.path.isdir(outdir):
                os.makedirs(outdir)
            for location in dataset.locations:
                if location.site == site:
                    command = "scp %s:%s %s" \
                              % (user_host, location.resource, output)
                    print command
                    if not dryrun:
                        if os.path.isfile(output) and clobber:
                            os.remove(output)
                        if not os.path.isfile(output):
                            subprocess.call(command, shell=True)
                        else:
                            print "%s already exists." % output
                    break  # Just need one location at this site.

class DataCatalogException(RuntimeError):
    def __init__(self, value):
        super(DataCatalogException, self).__init__(value)

class DataCatalog(object):
    def __init__(self, folder=None, experiment="LSST", 
                 mode="dev", remote_login=None, config_url=None):
        self.folder = folder
        if remote_login is None:
            self.remote_login = os.getlogin()
        # CONFIG_URL checks validity of experiment and mode values.
        my_config_url = datacat.config.CONFIG_URL(experiment, mode=mode)
        if my_config_url is None:
            raise DataCatalogException("Invalid experiment or mode: %s, %s"
                                       % (experiment, mode))
        if config_url is not None:
            # Override the computed value.
            my_config_url = config_url
        self.client = datacat.Client(my_config_url)
    def find_datasets(self, query, folder=None, pattern='**'):
        if folder is not None:
            self.folder = folder
        pattern_path = os.path.join(self.folder, pattern)
        what = "Failed datacat query:\n pattern = '%s'\n query = '%s'" \
               % (pattern, query)
        try:
            resp = self.client.search(pattern_path, query=query)
        except datacat.client.DcException:
            raise DataCatalogException(what)
        if resp.status_code != 200:
            raise DataCatalogException(what + "\n Status Code: " 
                                       + resp.status_code)
        return DatasetList(datacat.unpack(resp.content), self)

if __name__ == '__main__':
    # URL to use if tunneling from outside SLAC firewall using
    #
    # Host tunnel 
    #      ...
    #      LocalForward 8180 scalnx-v04.slac.stanford.edu:8180
    #
    # in .ssh/config.
    #config_url = "http://localhost:8180/org-srs-webapps-datacat-0.2-SNAPSHOT/r"

    folder = '/LSST/mirror/BNL3'
    query = 'TEMP_SET==-125 && TESTTYPE="DARK"'

    datacatalog = DataCatalog(folder=folder,
                              #config_url=config_url,
                              experiment='LSST')

    datasets = datacatalog.find_datasets(query)
    print "%i datasets found\n" % len(datasets)

    datasets.download(dryrun=True, clobber=False, nfiles=5)
