import hashlib
from samweb_client import *

samweb = SAMWebClient()

def locateOneLocalFile( aa, v=False ):
    loca = samweb.locateFile(aa)
    if len(loca) == 0 :
        if v :
            print "File has no location"
        return 10, ""
    if len(loca) > 1 :
        if v:
            print "ERROR - file has multiple locations, can't handle that"
        return 2,""
    loc = loca[0]
    lstr = loc['location'].split(':')[-1].split('(')[0]
    return 0,lstr

def sam2pnfs( args ):

    if(len(args) != 1 ):
        print "ERROR - must have one argument as input"
        return 1
    aa = args[0]
    na = len(aa.split("."))
    #print na," ",aa
    if na < 5 or na >6:
        print "ERROR - argument must be a dataset or file name (5 or 6 dot fields)"
        return 1

    if na == 5 :
        dims = "dh.dataset="+aa
        list = samweb.listFiles(dims, fileinfo=True, stream=False)
        first = list[0]
        ret = locateOneLocalFile(first[0], True)
        if ret[0] != 0 :
            return 1
        base = "/".join(ret[1].split('/')[0:-2])
        #print "base=",base
        test = ret[1].split('/')[3]
        oldStyle = True
        if test in ['tape','disk','persistent','scratch']:
            oldStyle = False
        #print "test=",test
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
            print base+'/'+spr+'/'+name

    elif na == 6 :
        # a single file, run samweb locate-file
        ret = locateOneLocalFile(aa, True)
        if ret[0] == 0:
            print ret[1]+"/"+aa

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
#            print self.samweb.parseDims(dims, mode)
#        elif options.summary:
#            summary = self.samweb.listFilesSummary(dims)
#            print _file_list_summary_str(summary)
#        else:
#            fileinfo = options.fileinfo
#            for l in _file_list_str_gen(
#self.samweb.listFiles(dims,fileinfo=fileinfo, stream=True), fileinfo):
#                print l
