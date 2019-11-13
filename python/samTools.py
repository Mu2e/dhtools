#from samweb_client import *
import samweb_client
#from samweb_cli import *
import samweb_cli

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
            print(traceback.print_exc())
        #print(sys.exc_info()[2].print_exc())
        err = True
    if verbose>1:
        print("ShellCommand result = ",res)

    return (err,rc,res)

########################################################################

def mu2eFiles(spec, fileInfo=False, limit=10000000 ):
    '''
    a function to make a list of files using a file name, a datatset,
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



####################################################################

class noChildren():
    '''
    sam command class to count how many files don't have children
    '''

    def help(self):
        '''
        print the help message for this command
        '''
        print('''
    sam [OPTIONS] no-children PARENTDATASET CHILDDATASET

        Count or list the file of a parent dataset that don't
        have children in the child dataset.
        Arguments must include the parent and child dataset.

        no-children options:
          -c print only file counts of input and output datasets
          -s print only sam file names (faster), not the full /pnfs names
          -f FILE File contains a list of the sam names of the parent 
                   dataset to operate on, instead of the whole dataset
        ''')


    def run(self, args ):
        '''
        execute the no-children command, to count files in parent
        daatset with no-children in child dataset
        args = the switches and dataset
        '''

        #print(len(args),args)
        options, remainder = getopt.getopt(args, 'n:d:', 
        ["limit=","directory="])

        if( len(remainder) != 1 ):
            print >> sys.stderr, "ERROR: get-files expects one argument as input"
            return 1

        nlimit = 100
        diry = "."
        for opt in options:
            if opt[0]=="-n" or opt[0]=="--limit":
                nlimit = int(opt[1])
            if opt[0]=="-d" or opt[0]=="--directory":
               diry = opt[1]

        return 0



####################################################################

class onDisk():
    '''
    copy some files locally
    '''

    def help(self):
        '''
        print the help message for this command
        '''
        print('''
    sam [OPTIONS] get-files [ARGS]

        Copy some files locally
        ARGS must be a file name, a dataset name, a dataset definition,
        or a samweb dims statement.  There is a built-in limit at 100
        files to prevent this being used in place of ifdh.

        on-tape options:
           -n (--limit=) INT   specify a limit on how many files to copy
           -d (--directory=) DIR   specify the output directory
        ''')


    def run(self, args ):
        '''
        execute the on-tape command, to count files on tape
        args = list of the args to the command
        '''

        #print(len(args),args)
        options, remainder = getopt.getopt(args, 'n:d:', 
        ["limit=","directory="])

        if( len(remainder) != 1 ):
            print >> sys.stderr, "ERROR: get-files expects one argument as input"
            return 1

        nlimit = 100
        diry = "."
        for opt in options:
            if opt[0]=="-n" or opt[0]=="--limit":
                nlimit = int(opt[1])
            if opt[0]=="-d" or opt[0]=="--directory":
               diry = opt[1]

        return 0



####################################################################

class fileRm():
    '''
    copy some files locally
    '''

    def help(self):
        '''
        print the help message for this command
        '''
        print('''
    sam [OPTIONS] get-files [ARGS]

        Copy some files locally
        ARGS must be a file name, a dataset name, a dataset definition,
        or a samweb dims statement.  There is a built-in limit at 100
        files to prevent this being used in place of ifdh.

        on-tape options:
           -n (--limit=) INT   specify a limit on how many files to copy
           -d (--directory=) DIR   specify the output directory
        ''')


    def run(self, args ):
        '''
        execute the on-tape command, to count files on tape
        args = list of the args to the command
        '''

        #print(len(args),args)
        options, remainder = getopt.getopt(args, 'n:d:', 
        ["limit=","directory="])

        if( len(remainder) != 1 ):
            print >> sys.stderr, "ERROR: get-files expects one argument as input"
            return 1

        nlimit = 100
        diry = "."
        for opt in options:
            if opt[0]=="-n" or opt[0]=="--limit":
                nlimit = int(opt[1])
            if opt[0]=="-d" or opt[0]=="--directory":
               diry = opt[1]

        return 0





####################################################################

class getFiles():
    '''
    copy some files locally
    '''

    def help(self):
        '''
        print the help message for this command
        '''
        print('''
    sam [OPTIONS] get-files [ARGS]

        Copy some files locally
        ARGS must be a file name, a dataset name, a dataset definition,
        or a samweb dims statement.  There is a built-in limit at 100
        files to prevent this being used in place of ifdh.

        on-tape options:
           -n (--limit=) INT   specify a limit on how many files to copy
           -d (--directory=) DIR   specify the output directory
        ''')


    def run(self, args ):
        '''
        execute the on-tape command, to count files on tape
        args = list of the args to the command
        '''

        #print(len(args),args)
        options, remainder = getopt.getopt(args, 'n:d:', 
        ["limit=","directory="])

        if( len(remainder) != 1 ):
            print >> sys.stderr, "ERROR: get-files expects one argument as input"
            return 1

        nlimit = 100
        diry = "."
        for opt in options:
            if opt[0]=="-n" or opt[0]=="--limit":
                nlimit = int(opt[1])
            if opt[0]=="-d" or opt[0]=="--directory":
               diry = opt[1]
        if nlimit>100:
            nlimit = 100

        # use utility function to interpret the filespec
        # this returns sam names
        list = mu2eFiles(remainder[0],False,nlimit)

        #print(list)
        #return 0


        # now locate them
        cmdlist = []
        samweb = samweb_client.SAMWebClient()
        for fn in list:
            loca = samweb.locateFile(fn)
            if len(loca) == 0 :
                print >> sys.stderr, "ERROR: {0:s} has no location".format(fn)
            else:
                loc = loca[0]
                lstr = loc['location'].split(':')[-1].split('(')[0]
                cmdlist.append(lstr+"/"+fn+" "+diry+"/"+fn+"\n")

        # write out the commands to a temp file
        ret = ShellCommand("mktemp", 1)
        if ret[0] or ret[1]!=0:
            print >> sys.stderr, "ERROR: could not make a temp file"
            return 1
        #print(ret[2])

        # mktemp comes back with a teminal \n
        tempFileName = ret[2].replace("\n","")
        tempFile = open(tempFileName, "w")
        for line in cmdlist: 
            tempFile.write(line)
        tempFile.close()

        #print("cat ",tempFile.name)
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
        #print(cmd)
        #print(ret[2])

        if ret[0] or ret[1]!=0 :
            print >> sys.stderr, "ERROR: could not run ifdh cp to move files"
            return 2

        # remove temp file
        cmd = "rm -f "+tempFileName
        ret = ShellCommand(cmd, 1)

        return 0




#
# a class to execute the command on-tape which takes 
# a dataset names and prints how many have a tape location
#

class onTape():
    '''
    count files in a dataset that have a tape location
    '''

    def help(self):
        '''
        print the help message for this command
        '''
        print('''
    sam [OPTIONS] on-tape [ARGS]

        Count files in a dataset that have a tape location
        ARGS must be a dataset name

        on-tape options:
           none
        ''')


    def run(self, args ):
        '''
        execute the on-tape command, to count files on tape
        args = list of the args to the command
        '''

        if( len(args) != 1 ):
            print >> sys.stderr, "ERROR: on-tape expects one argument as input"
            return 1

        aa = args[0]
        nd = len(aa.split("."))

        if( nd != 5 ):
            print >> sys.stderr, "ERROR: on-tape expects a dataset name as input"
            return 1

        self.samweb = samweb_client.SAMWebClient()
        dims = "dh.dataset="+args[0]
        # get the full list of files
        summary = self.samweb.listFilesSummary(dims)
        dims = dims+" and tape_label %"
        # now the ones on tape
        summary_tape = self.samweb.listFilesSummary(dims)
        nt = summary["file_count"]
        no = summary_tape["file_count"]
        print("{0:6d} total  {1:6d} on tape   {2:6d} not on tape".format(nt,no,nt-no))
        return 0

#
# a class to execute the command to-pnfs which takes 
# a specification for a set of files and prints a list of full filespecs
#

class toPnfs():
    '''
    translate a file name or set of file into a list of full local filespecs
    '''

    def help(self):
        '''
        print the help message for this command
        '''
        print('''
    sam [OPTIONS] to-pnfs [ARGS]

        Print the full filespec for all the requested files.
        ARGS may be a single file name, a dataset name, a dataset 
        definition or a samweb dims selection statement in double quotes.

        to-pnfs options:
           none
''')


    def run(self, args ):
        '''
        execute the toPnfs command, to make a list of filespecs
        args = list of the args to the command
        '''

        #print(len(args),args)
        #options, remainder = getopt.getopt(sys.argv[1:], 'h', 
        #['help'])

        if( len(args) != 1 ):
            print >> sys.stderr, "ERROR: to-pnfs expects one argument as input"
            return 1

        aa = args[0]
        # number of all types of fields
        nf = len(aa.replace(":"," ").replace("="," ").split())
        # number of fields delimited by dots
        nd = len(aa.split("."))
        #print(na," ",aa)

        self.samweb = samweb_client.SAMWebClient()
     
        if nf==1 and nd == 6 :
            # a single file, run samweb locate-file
            ret = self.locateOneLocalFile(aa, True)
            if ret[0] == 0:
                print(ret[1]+"/"+aa)
        else :
            if nf==1 and nd == 5:
                # just a  dataset name
                dims = "dh.dataset="+aa
            elif nf==1 and nd<5:
                # probably a dataset def
                dims = "defname : "+aa
            else: # dataset def or more complicated samweb selection
                dims = aa

            # get the full list of files
            list = self.samweb.listFiles(dims, fileinfo=True, stream=False)
            # locate the first only
            first = list[0]
            ret = self.locateOneLocalFile(first[0], True)
            if ret[0] != 0 :
                return ret[0]
            # now loop over file and print calculated file
            base = "/".join(ret[1].split('/')[0:-2])
            test = ret[1].split('/')[3]
            oldStyle = True
            if test in ['tape','disk','persistent','scratch']:
                oldStyle = False
            for line in list:
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
                print(base+'/'+spr+'/'+name)
     
        return 0

    def locateOneLocalFile(self, aa, v=False ):
        '''
        run samweb locate-file on one file name
        '''
        loca = self.samweb.locateFile(aa)
        if len(loca) == 0 :
            if v :
                print("File has no location")
            return 10, ""
        if len(loca) > 1 :
            if v:
                print("ERROR: file has multiple locations, can't handle that")
            return 2,""
        loc = loca[0]
        lstr = loc['location'].split(':')[-1].split('(')[0]
        return 0,lstr
    


#
# the main entry pont to prase command and either run the 
# mu2e command or pass it on to samweb
#

class mu2eCommands():
    '''
      execute sam commands that mu2e has added to samweb, otherwise
      pass it on to samweb
    '''

    def __init__(self):
        self.samweb = samweb_client.SAMWebClient()
        #samweb_cli._list_commands("","","","")

    def help(self):
        '''
        print help
        '''
        print("""

    sam COMMAND [OPTIONS] [ARGS]

    The command 'sam' is a set of procedures that add to the 
    the standard sam_web_client 'samweb' commands.  If sam does
    not recognize the command, it passes it to samweb, so the
    sam command can be used for the standard commands and the additions.

Available mu2e commands:
    to-pnfs
    get-files
    no-children
    on-disk
    on-tape
    rm-files

    """)
        print(samweb_cli.command_list())

        return

    def command_list(self):
        return ["to-pnfs","on-tape","get-files"]

    def run(self,args):
        runHelp = False
        bargs = []
        for ww in args:
            if ww=="-h" or ww=="--help" or ww=="help":
                runHelp = True
            else:
                bargs.append(ww)

        cmds = samweb_cli.command_list().split()+self.command_list()
        cmdName = ""
        cargs = []
        for ww in bargs:
            if ww in cmds:
                cmdName = ww
            else:
                cargs.append(ww)

        if cmdName=="":
            self.help()
            return 1

        if cmdName not in self.command_list():
            # run it in samweb
            rc = samweb_cli.main(bargs)
        else:
            if cmdName=="to-pnfs":
                cmd = toPnfs()
                if runHelp:
                    cmd.help()
                else:
                    cmd.run(cargs)
            if cmdName=="on-tape":
                cmd = onTape()
                if runHelp:
                    cmd.help()
                else:
                    cmd.run(cargs)
            if cmdName=="get-files":
                cmd = getFiles()
                if runHelp:
                    cmd.help()
                else:
                    cmd.run(cargs)


        return



    #dims="dh.dataset bck.mu2e.tdr-beam-g4s2.1025a_1025a.tgz"
#
#def _file_list_str_gen(g, fileinfo):
#    if fileinfo:
#        for result in g:
#            yield '\t'.join(str(e) for e in result)
#    else:
#        for result in g:
#            yield result
#
#c
#
#class listFilesCmd(CmdBase):
#    name = "list-files"
#    options = [ ("dump-query", "Return query information for these dimensions instead of evaluating them"),
#                ("fileinfo", "Return additional information for each file"),
#                ("summary", "Return a summary of the results instead of the full list"),
#                ("help-dimensions", "Return information on the available dimensions"),
#                ]
#
#    description = "List files by dimensions query"
#    cmdgroup = 'datafiles'
#    args = "<dimensions query>"
#
#    def run(self, options, args):
#        if options.help_dimensions:
#            _help_dimensions(self.samweb)
#            return
#        dims = (' '.join(args)).strip()
#        if not dims:
#            raise CmdError("No dimensions specified")
#        if options.dump_query:
#            if options.summary: mode = 'summary'
#            else: mode = None
#            print(self.samweb.parseDims(dims, mode))
#        elif options.summary:
#            summary = self.samweb.listFilesSummary(dims)
#            print(_file_list_summary_str(summary))
#        else:
#            fileinfo = options.fileinfo
#            for l in _file_list_str_gen(
#self.samweb.listFiles(dims,fileinfo=fileinfo, stream=True), fileinfo):
#                print(l)
