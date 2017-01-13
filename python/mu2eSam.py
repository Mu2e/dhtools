#from samweb_client import *
import samweb_client
#from samweb_cli import *
from samweb_cli import CmdBase,CmdError

import getopt
import sys
import hashlib
import traceback

########################################################################

def ShellCommand(cmd, verbose=1):
    '''
    a utility function to perform a shell command
    '''
    import subprocess

    err = False
    res = ""
    rc = 0
    try:
        res = subprocess.check_output(cmd,shell=True)
    except subprocess.CalledProcessError as cpe:
        rc = cpe.returncode
        if verbose>0 :
            print >> sys.stderr, "ERROR: ShellCommand executable return code ",rc
    except:
        if verbose>0 :
            print >> sys.stderr, "ERROR: ShellCommand subprocess call reports a method error "
            print traceback.print_exc()
        #print sys.exc_info()[2].print_exc()
        err = True
    if verbose>1:
        print "ShellCommand result = ",res

    return (err,rc,res)

########################################################################

def mu2eFiles(spec, fileInfo=False, limit=10000000 ):
    '''
    a utility function to make a list of files using a file name, a datatset,
    a dataset definition, or a samweb dimension statement
    '''

    # number of all types of fields
    nf = len(spec.replace(":"," ").replace("="," ").split())
    # number of fields delimited by dots
    nd = len(spec.split("."))

    samweb = samweb_client.SAMWebClient()
 
    if nf==1 and nd==6 :
        dims = "file_name="+spec
    else :
        if nf==1 and nd==5:
            # just a  dataset name
            dims = "dh.dataset="+spec
        elif nf==1 and nd<5:
            # probably a dataset def
            dims = "defname : "+spec
        else: # dataset def or more complicated samweb selection
            dims = spec

    dims = dims+" with limit {0:d}".format(limit)
    # get the list
    list = samweb.listFiles(dims, fileinfo=fileInfo, stream=False)

    return list

########################################################################

def mu2eLocate(spec):
    '''
    a utility function to find pnfs locations of a list of files
    if a file doesn't have a location, don't return an entry for it
    the spec is a file name, a dataset or a datasetdef
    '''

    flist = mu2eFiles(spec, True)
    # locate the first only
    testList = []
    testList.append(flist[0].file_name)
    testLocList = mu2eLocateList(testList)
    testLoc = testLocList[0]
    #print testLoc
    test = testLoc.split('/')[3]
    oldStyle = True
    if test in ['tape','disk','persistent','scratch']:
        oldStyle = False

    # now loop over file and print calculated file
    base = "/".join(testLoc.split('/')[0:-3])
    #print base

    ret = []
    for line in flist:
        name = line[0]
        if oldStyle:
            x = int(line[1])
            x1 = x/1000000
            x2 = x/1000
            spr = "{0:03d}/{1:03d}".format(x1,x2)
        else:
            hasher = hashlib.sha256()
            hasher.update(name)
            hh = hasher.hexdigest()
            spr = hh[0:2]+'/'+hh[2:4]
        pname = base+'/'+spr+'/'+name
        ret.append(pname)

    return ret


########################################################################

def mu2eLocateList(list):
    '''
    a utility function to find pnfs locations of a list of files
    if a file doesn't have a location, don't return an entry for it
    '''
    samweb = samweb_client.SAMWebClient()
    ret = []
    for fn in list:
        # locList is a list of locations
        locList = samweb.locateFile(fn)
        # find location in enstore - if more than one, take first
        for loc in locList:
            if loc['system'] == "enstore" :
                lstr = loc['location'].split(':')[-1].split('(')[0]
                ret.append(lstr+"/"+fn)
                break

    return ret


#####################################################################
#
#class samRm(CmdBase):
#    '''
#    delete a file in 
#    '''
#    name = 'sam-rm'
#    description = 'count files in a dataset that have a tape location'
#    cmdgroup = 'mu2e commands'
#
#    options = []
#    args = "<dataset>"
#
#    def run(self, options, args):
#        if not (len(args)==1):
#            raise CmdError("wrong number of arguments")
#
#        aa = args[0]
#        nd = len(aa.split("."))
#
#        if( nd != 5 ):
#            raise CmdError("on-tape expects a dataset name as input")
#
#        self.samweb = samweb_client.SAMWebClient()
#        dims = "dh.dataset="+args[0]+" with availability anylocation"
#        # get the full list of files
#        summary = self.samweb.listFilesSummary(dims)
#        dims = "dh.dataset="+args[0]+" and tape_label %"
#        # now the ones on tape
#        summary_tape = self.samweb.listFilesSummary(dims)
#        nt = summary["file_count"]
#        no = summary_tape["file_count"]
#        print "{0:6d} total  {1:6d} on tape   {2:6d} not on tape".format(nt,no,nt-no)
#        return 0


####################################################################

class onDisk(CmdBase):
    '''
    probe a dataset, up to 1000 files, to get a sense of
    what fraciton of files are in dCache.  It is too expensive
    to run on all files.
    '''
    name = 'on-disk'
    description = 'approximate count of files in a dataset that are in dCache'
    cmdgroup = 'mu2e commands'

    options = []
    args = "<filename|dataset|datasetdef|--samlist>"

    def addOptions(self, parser):
        parser.add_option("--samlist", action="store", dest="samlist", 
              help="A file containing sam names of files")

    def run(self, options, args):

        import random

        if not (len(args)==1 or (len(args)==0 and options.samlist is not None)):
            raise CmdError("wrong number of arguments for file specification")


        fl = []
        if options.samlist is not None:
            fp = open(options.samlist,"r")
            fl = fp.read().split()
            fp.close()
        else:
            fl = mu2eFiles(args[0])

        # only run on up to 1000
        lim = len(fl)
        if lim > 1000 : lim = 1000

        # randomize the list
        random.shuffle(fl)
        flshort = fl[:lim]
        pfl = mu2eLocateList(flshort)

        # loop in random order and check if they are in dCache
        nt = 0
        nd = 0
        for pfn in pfl:
            fn = pfn.split("/")[-1]
            ll = pfn.split("/")[0:-1]
            dr = "/"+"/".join(ll)
            cmd = "cat "+dr+"/'.(get)("+fn+")(locality)'"
            cmdRet = ShellCommand(cmd)
            stat = cmdRet[2]
            nt = nt + 1
            if "ONLINE" in stat: nd = nd +1
            frac = 100.0*nd/float(nt)
            print "{0:4d}/{1:4d} are on disk, {2:4.1f}%".format(nd,nt,frac)

        return 0




####################################################################

class getFiles(CmdBase):
    '''
    copy a few sam files locally
    '''
    name = 'get-files'
    description = 'copy a few sam files locally'
    cmdgroup = 'mu2e commands'

    options = []
    args = "<filename|dataset|datasetdef|--samlist>"

    def addOptions(self, parser):
        parser.add_option("--samlist", action="store", dest="samlist", 
              help="A file containing sam names of files")
        parser.add_option("--limit", action="store", dest="nlimit", 
              default="100", help="the limit on number of files to copy (<100)")
        parser.add_option("--directory", action="store", dest="dir", 
              default=".",help="directory for output (default=PWD)")

    def run(self, options, args):
        if not (len(args)==1 or (len(args)==0 and options.samlist is not None)):
            raise CmdError("wrong number of arguments for file specification")


        nlimit = int(options.nlimit)
        if nlimit>100 :
            nlimit = 100
        diry = options.dir

        # use utility function to interpret the filespec
        # this returns sam names
        list = mu2eFiles(args[0],False,nlimit)

        #print list
        #return 0


        # the list of ifdh commands
        cmdlist = []
        #samweb = samweb_client.SAMWebClient()

        # find full file specs
        locList = mu2eLocateList(list)
        for pn in locList:
            fn = pn.split('/')[-1] 
            cmdlist.append(pn+" "+diry+"/"+fn+"\n")

        # write out the commands to a temp file
        ret = ShellCommand("mktemp", 1)
        if ret[0] or ret[1]!=0:
            raise CmdError("failed to make a temp file")
            
        #print ret[2]

        # mktemp comes back with a teminal \n
        tempFileName = ret[2].replace("\n","")
        tempFile = open(tempFileName, "w")
        for line in cmdlist: 
            tempFile.write(line)
        tempFile.close()

        #print "cat ",tempFile.name
        #return 1

        # execute a bulk ifdh transfer
        cmd = "source /cvmfs/fermilab.opensciencegrid.org/products/common/etc/setups"
        cmd = cmd+ " ; "
        cmd = cmd+ "[ -n \"$SETUP_PYTHON\" ] && unsetup python"
        cmd = cmd+ " ; "
        cmd = cmd+ "setup ifdhc"
        cmd = cmd+ " ; "
        cmd = cmd+ "ifdh cp -f "+tempFileName

        ret = ShellCommand(cmd, 1)
        #print cmd
        #print ret[2]

        if ret[0] or ret[1]!=0 :
            raise CmdError("failed to run ifdh to move files")

        # remove temp file
        cmd = "rm -f "+tempFileName
        ret = ShellCommand(cmd, 1)

        return 0



####################################################################

class onTape(CmdBase):
    '''
    count files in a dataset that have a tape location
    '''
    name = 'on-tape'
    description = 'count files in a dataset that have a tape location'
    cmdgroup = 'mu2e commands'

    options = []
    args = "<dataset>"

    def run(self, options, args):
        if not (len(args)==1):
            raise CmdError("wrong number of arguments")

        aa = args[0]
        nd = len(aa.split("."))

        if( nd != 5 ):
            raise CmdError("on-tape expects a dataset name as input")

        self.samweb = samweb_client.SAMWebClient()
        dims = "dh.dataset="+args[0]+" with availability anylocation"
        # get the full list of files
        summary = self.samweb.listFilesSummary(dims)
        dims = "dh.dataset="+args[0]+" and tape_label %"
        # now the ones on tape
        summary_tape = self.samweb.listFilesSummary(dims)
        nt = summary["file_count"]
        no = summary_tape["file_count"]
        print "{0:6d} total  {1:6d} on tape   {2:6d} not on tape".format(nt,no,nt-no)
        return 0



####################################################################

class toPnfs(CmdBase):
    '''
    sam command class to count how many files don't have children
    '''
    name = 'to-pnfs'
    description = 'list pnfs location of files specified as a file name,\na dataset name, a dataset definition or file containing sam file names'
    cmdgroup = 'mu2e commands'

    options = []
    args = "<filename|dataset|datasetdef|--samlist>"

    def addOptions(self, parser):
        parser.add_option("--samlist", action="store", dest="samlist", 
              help="A file containing sam names of files")


    def run(self, options, args):
        if not (len(args)==1 or (len(args)==0 and options.samlist is not None)):
            raise CmdError("wrong number of arguments for file specification")

        fpl = []
        if options.samlist is not None:
            fp = open(options.samlist,"r")
            fl = fp.read().split()
            fp.close()
            fpl = mu2eLocateList(fl)
        else:
            fpl = mu2eLocate(args[0])
        # print full paths
        for fn in fpl:
            print fn
        return 0


####################################################################

class noChildren(CmdBase):
    '''
    sam command class to count how many files don't have children
    '''
    name = 'no-children'
    description = 'find parent files missing their child files'
    cmdgroup = 'mu2e commands'

    options = [ ("path", "print path/name, not just name (slower)"),
                ("summary", "Return a summary of the results instead of the full list")
                ]
    args = "<parent dataset or --samlist> <child dataset> "

    def addOptions(self, parser):
        parser.add_option("--samlist", action="store", dest="samlist", 
              help="A file containing sam names of parent files")

    def run(self, options, args):

        #print options.samlist
        #print len(args),args
        if (len(args)>2 or (len(args)>1 and options.samlist is not None)):
            raise CmdError("too many parent/child specifiers")
        if not (len(args)==2 or (len(args)==1 and options.samlist is not None)):
            raise CmdError("parent or child dataset not specified")

        np = 0
        pl = []
        if options.samlist is not None:
            fp = open(options.samlist,"r")
            pl = fp.read().split()
            fp.close()
            np = len(pl)
            arr = pl[0].split(".")
            parentDS = arr[0]+"."+arr[1]+"."+arr[2]+"."+arr[3]+"."+arr[5]
            childDS = args[0]
        else:
            parentDS = args[0]
            childDS = args[1]

        # find count of parents with no children, or the list of same
        nl = []  # parent with no children
        nn = 0   # number of same
        if options.samlist is not None :
            # loop over only requested parents
            for pf in pl:
                dims="file_name="+pf+\
                    " and not  isparentof:(dh.dataset="+childDS+")"
                cc = self.samweb.listFiles(dims)
                if len(cc)>0 :
                    nl.append(cc[0])
                    nn = nn +1
                
        else:
            # use dataset specifier to find counts or lists
            pdims="dh.dataset="+parentDS
            cdims="dh.dataset="+childDS
            ndims="dh.dataset="+parentDS+\
                " and not  isparentof:(dh.dataset="+childDS+")"
            np = self.samweb.countFiles(pdims)
            if options.summary:
                # just do the counts
                nn = self.samweb.countFiles(ndims)
            else:
                #get the list
                nl = self.samweb.listFiles(ndims)
                nn = len(nl)

        if options.summary:
            # just print summary and quit
            nc = np - nn
            print "parent: {0:d}  child: {1:d}   parentNoChild: {2:d}".\
                format(np,nc,nn)
            return 0

        # now have the list of child files
        if not options.path :
            # just the file names
            for sn in nl:
                print sn
            return 0
        # if here, then print full paths
        fl = mu2eLocate(nl)
        # print the path/file names
        for fn in fl:
            print fn
        return 0




