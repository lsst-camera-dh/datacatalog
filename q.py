#!/usr/bin/env python2.7

## Convenience script to query for LSST Sensor Test Data from the
## SLAC-hosted data catalog.

## T.Glanzman  4/10/2014

import os,sys
import shlex
from subprocess import Popen, PIPE, STDOUT
import argparse



## Command line arguments
parser = argparse.ArgumentParser(description='query the SRS dataCatalog')

## Parameters (optional unless "required")
parser.add_argument('-r','--remoteUser',default=None,help="remote (SLAC) username to use")
parser.add_argument('-R','--remotePath',default=None,help="remote path root, e.g., /astro/astronfs01/ccdtest")


## FT1 metadata filter opts
parser.add_argument('-f','--filter',default=None,help="FT1 metadata filter string (default=%(default)s)")
##   The following are 'convenience options' which could also be specified in the filter string
parser.add_argument('-t','--timestamp',default=None,help="(metadata) File timestamp")
parser.add_argument('-s','--sensorID', default=None,help="(metadata) Sensor ID")
parser.add_argument('-T','--TestType', default=None,help="(metadata) test type")

## Limit dataCatalog search to specified parts of the catalog
parser.add_argument('-g','--group',default=None,help="Limit search to specified dataCat group (default=%(default)s)")
parser.add_argument('-m','--mirrorName',default='BNL3',help="mirror name to search, i.e. in dataCat /LSST/mirror/<mirrorName> (default=%(default)s)")
parser.add_argument('-X','--XtraOpts',default='',help="any extra 'datacat find' options")

## Output
parser.add_argument('-o','--outputFile',default=None,help="Output result to specified file (default = %(default)s)")
parser.add_argument('-a','--displayAll',default=False,action='store_true',help="Display entire result set (default = %(default)s)")

## Verbosity
parser.add_argument('-d','--debug',default=False,action='store_true',help="enable debug mode")
parser.add_argument('-x','--dryRun',default=False,action='store_true',help="dry run (no DB action)")


args = parser.parse_args()

debug = args.debug
dryrun = args.dryRun

if debug:                    ## Diagnostic dump of command line args
    argsd = vars(args)
    argkeys = argsd.keys()
    print '\nSupplied arguments:'
    for arg in argkeys:
        print '  ',arg,': ',argsd[arg]
        pass
    print
    pass

####################################################################################

##  Map of FT1 header metadata names with script args (see LCA-10140)
# FT1map = [
#     ('LSST_NUM',args.sensorID),
#     ('DATE',args.timestamp),
#     ('TESTTYPE',args.TestType)
#     ]
FT1map = [
    ('CCD_SER',args.sensorID),
    ('DATE',args.timestamp),
    ('TESTTYPE',args.TestType)
    ]

## Tools and Locations
slacHost = 'rhel6-64.slac.stanford.edu'                                       ## ssh target
dcCmd = '/u/lt/lsstsim/datacat/prod/datacat find --recurse --search-groups --search-folders '  ## base datacat command
mirrorName = '/LSST/mirror/'+args.mirrorName                            ## default logical directory to search
slacRoot = '/nfs/farm/g/lsst/u1/mirror/'+args.mirrorName      ## Location of mirror at SLAC

####################################################################################

## Assemble filter string
cmdOpts = []
for opt,val in FT1map:
    if val != None:
        cmdOpts.append('%s=="%s"' % (opt,val))
        pass
    pass
if args.filter != None:
    cmdOpts.append(args.filter)
    pass

cmdOpts = ' --filter ' + "'" + ' && '.join(cmdOpts) + "'"
#if debug: print 'cmdOpts (filter string) = ',cmdOpts

## Build up remaining command options
if args.group != None:
    cmdOpts += ' --group '+args.group
    pass

## xtraBits are 'hardwired' extra options to be added to each and
## every 'datacat find' command
#xtraBits = ' --display MONO_FILTER --display OD1_R '
xtraBits = ''

cmd = dcCmd+' '+cmdOpts+' '+xtraBits+' '+args.XtraOpts+' '+mirrorName

if args.remoteUser != None:
    cmd = 'ssh '+args.remoteUser+'@rhel6-64.slac.stanford.edu '+cmd
    pass


print "\nIssuing the following command:"
print "$ ",cmd

## Spawn subprocess and run command
if args.remoteUser == None:
    cmdList = shlex.split(cmd)     ## Domestic command
else:
    cmdList = cmd.split()          ## Foreign command via ssh
    pass

if not dryrun:
    so = Popen(cmdList, stdout=PIPE, stderr=PIPE)
##     fetch output
    response = so.communicate()
    normalOut = response[0]
    errOut = response[1]
else:
    print "\n    DRY RUN!"
    normalOut= []
    errOut = []
    pass


## Problems with datacat find?
if len(errOut) > 0:
    errLines = errOut.splitlines()
    print '\n%STDERR (',len(errLines),' lines): '
    if debug or args.displayAll:
        lim = len(errLines)
    else:
        lim = min(10,len(errLines))
        pass
    for indx in range(lim):
        print errLines[indx]
        pass
    pass

## Generate file list, if any
fileList = []
if len(normalOut) > 0:
    fileList = normalOut.splitlines()
    fileList.sort()
    ## Replace SLAC path with remote path, if requested
    if args.remotePath != None:
        for indx in range(len(fileList)):
            fileList[indx]=args.remotePath+'/'+fileList[indx].lstrip(slacRoot)
            pass
        pass
        
    if args.displayAll:
        lim = len(fileList)
    else:
        lim = min(5,len(fileList))
        pass
    print "There were ",len(fileList)," files found, with the first ",lim," listed below."
    for indx in range(lim):
        print fileList[indx]
        pass
else:
    print "There were no files found by 'datacat find'"
    pass

## Write file with list of found data files, if requested
if args.outputFile != None and len(fileList)>0:
    print 'Writing output file ',args.outputFile,'...'
    ofile = open(args.outputFile,'w')
    ofile.write("# "+cmd+"\n")
    ofile.write("# There were "+str(len(fileList))+" files found.\n")
    for line in fileList:
        ofile.write(line+'\n')
        pass
    ofile.close()
elif args.outputFile != None:
    print "Result file requested, but no files found"
    pass


"""
Ref:  https://confluence.slac.stanford.edu/display/SRSPDC/Line-mode+Client+%27find%27+Command

Basic datacat command format:

$ srsdatacat find -h
Command-specific help for command find

Usage: datacat find [-options] <logical folder>

parameters:
  <logical folder>   Logical Folder Path at which to begin performing the search.

options:
  --recurse                      Recurse sub-folders
  --search-folders               Search for datasets inside folder(s)
  --search-groups                Search in groups.  This option is superseded by the -G (--group) option if they are both supplied.
  --group <group name>           Dataset Group under which to search for datasets.
  --site <site name>             Name of Site to search.  May be used multiple times to specify a list of sites in which case order is taken as preference.  Defaults to the Master-location if not provided.
  --filter <filter expression>   Criteria by which to filter datasets.  ie: 'DatasetDataType=="MERIT" && nMetStart>=257731220 && nMetStop <=257731580'
  --display <meta name>          Name of meta-data field to display in output.  Default is to display only the file location.  May be used multiple times to specify an ordered list of fields to display.
  --sort <meta name>             Name of meta-data field to sort on.  May be used multiple times to specify a list of fields to sort on.  Order determines precedence.
  --show-unscanned-locations     If no "OK" (ie: verified by file crawler) location exists, display first location (if any) which has not yet been scanned.  If this option and '--show-non-ok-locations' are both specified, an unscanned location will be returned before a non-ok location regardless of their sequence in the ordered site list.
  --show-non-ok-locations        If no "OK" (ie: verified by file crawler) location exists, display first location (if any) which exists in the list of sites.

==================================================================

$ srsdatacat -h
Usage: datacat [-options] <command>

parameters:
  <command>   Command to execute, one of:
	rm
	registerDataset
	mkdir
	showDataTypes
	addLocation
	addMetaData
	find
	showFileTypes

options:
  --help               Show this help page; or if <command> specified, show Command-Specific help
  --nocommit           If specified, command will not commit actions to database.  Useful for testing a command-line construction.
  --mode <mode=PROD>   Specify Data Source {PROD, DEV, TEST}

"""

