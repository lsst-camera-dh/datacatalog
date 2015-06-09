"""
Use Brian's python client to query the SRS datacatalog.
"""

import os
import subprocess
import datacat

remote_hosts = {'SLAC' : 'rhel6-64.slac.stanford.edu'}

class DatasetList(list):
    def __init__(self, my_list, datacat_obj, sort_by_name=True):
        if sort_by_name:
            super(DatasetList, self).__init__(sorted(my_list,
                                                     key=lambda x : x.name))
        else:
            super(DatasetList, self).__init__(my_list)
        self.folder = datacat_obj.folder
        self.login = datacat_obj.remote_login
        self.site = datacat_obj.site
    @property
    def filenames(self):
        return [str(x.name) for x in self]
    @property
    def full_paths(self):
        my_full_paths = []
        for dataset in self:
            for location in dataset.locations:
                if location.site == self.site:
                    break
            my_full_paths.append(str(location.resource))
        return my_full_paths
    def download(self, site='SLAC', rootpath='.', nfiles=None, dryrun=True,
                 clobber=False):
        user_host = '@'.join((self.login, remote_hosts[site]))
        if nfiles is not None:
            print "Downloading the first %i files:\n" % nfiles
        if dryrun:
            print "Dry run. The following commands would be executed:\n"
        my_datasets = []
        for dataset in self[:nfiles]:
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
                 mode="dev", remote_login=None, site='SLAC', config_url=None):
        self.folder = folder
        if remote_login is None:
            self.remote_login = os.getlogin()
        self.site = site
        my_config_url = datacat.config.default_url(experiment, mode=mode)
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
        resp = self.client.search(pattern_path, query=query)
        return DatasetList(resp, self)

if __name__ == '__main__':
    folder = '/LSST/mirror/BNL3'
    query = 'TEMP_SET==-125 && TESTTYPE="DARK"'
    site = 'SLAC'

    datacatalog = DataCatalog(folder=folder, experiment='LSST', site=site)

    datasets = datacatalog.find_datasets(query)
    print "%i datasets found\n" % len(datasets)

    nfiles = 5
    print "File paths for first %i files at %s:" % (nfiles, site)
    for i in range(nfiles):
        print datasets.full_paths[i]

    print

    datasets.download(dryrun=True, clobber=False, nfiles=nfiles)
