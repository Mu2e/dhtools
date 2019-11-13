

import math
import time
import sys
from operator import itemgetter

##################################
#
# classes for parseCondor
#
#################################


class condorCode:
    """A utility class containing all the condor message codes"""

    def __init__(self):
        self.code ={}
        self.code["000"] = "Job submitted from host"
        self.code["001"] = "Job executing on host"
        self.code["002"] = "executable error"
        self.code["003"] = "checkpointed"
        self.code["004"] = "Job was evicted"
        self.code["005"] = "Job terminated"
        self.code["006"] = "Image size of job updated"
        self.code["007"] = "Shadow exception"
        self.code["008"] = "generic"
        self.code["009"] = "Job was aborted by the user"
        self.code["010"] = "Job suspended"
        self.code["011"] = "Job unsuspended"
        self.code["012"] = "Job was held"
        self.code["013"] = "Job was released"
        self.code["014"] = "Node execute"
        self.code["015"] = "Node terminated"
        self.code["016"] = "Post script terminated"
        self.code["017"] = "Globus submit"
        self.code["018"] = "Globus submit failed"
        self.code["019"] = "Globus resource up"
        self.code["020"] = "Globus resource down"
        self.code["021"] = "Remote error"
        self.code["022"] = "Job disconnected, attempting to reconnect"
        self.code["023"] = "Job reconnected"
        self.code["024"] = "Job reconnection failed"
        self.code["028"] = "Job ad information event triggered."

        self.mess = {}
        self.mess["000"] = "submitted"
        self.mess["001"] = "executing"
        self.mess["002"] = "exeError"
        self.mess["003"] = "checkpoint"
        self.mess["004"] = "evicted"
        self.mess["005"] = "terminated"
        self.mess["006"] = "Updated"
        self.mess["007"] = "ShadowErr"
        self.mess["008"] = "generic"
        self.mess["009"] = "userAbort"
        self.mess["010"] = "suspended"
        self.mess["011"] = "unsuspended"
        self.mess["012"] = "held"
        self.mess["013"] = "released"
        self.mess["014"] = "nodeExecute"
        self.mess["015"] = "nodeTerminated"
        self.mess["016"] = "PSterminated"
        self.mess["017"] = "globSubmit"
        self.mess["018"] = "globFailed"
        self.mess["019"] = "globResUp"
        self.mess["020"] = "globResDown"
        self.mess["021"] = "remoteError"
        self.mess["022"] = "disconnected"
        self.mess["023"] = "reconnected"
        self.mess["024"] = "reconnectFailed"
        self.mess["028"] = "jobInfo"

        # if this messgae is the last encountered, what state is the job in?
        # noop means the state did not change
        self.state = {}
        self.state["000"] = "idle"
        self.state["001"] = "executing"
        self.state["002"] = "failed"
        self.state["003"] = "noop"
        self.state["004"] = "idle"
        self.state["005"] = "terminated"
        self.state["006"] = "noop"
        self.state["007"] = "idle"
        self.state["008"] = "noop"
        self.state["009"] = "failed"
        self.state["010"] = "suspended"
        self.state["011"] = "executing"
        self.state["012"] = "held"
        self.state["013"] = "idle"
        self.state["014"] = "noop"
        self.state["015"] = "idle"
        self.state["016"] = "noop"
        self.state["017"] = "noop"
        self.state["018"] = "noop"
        self.state["019"] = "noop"
        self.state["020"] = "noop"
        self.state["021"] = "idle"
        self.state["022"] = "executing"
        self.state["023"] = "executing"
        self.state["024"] = "idle"
        self.state["028"] = "noop"


class condorLog:
    """A utility class containing all lines of one condor log file"""

    def __init__(self, inFileName=""):
        self.inFile = inFileName;
        self.ll = []
        self.valid = False
        if len(inFileName) >0 :
            self.read(inFileName)

    def __str__(self):
        line = ""
        line += "{0:d} lines in {1:s}".format(len(self.ll),self.inFile)
        return line

    def read(self, inFileName=""):
        if len(inFileName) > 0:
            self.inFile = inFileName;
        try:
            f = open(self.inFile, 'r')
            self.ll = f.read().splitlines()
            f.close()
        except:
            print("\nError - could not read log file {0:s}!\n".\
                format(self.inFile))
            raise

class condorMsg:
    """A utility class containing one condor message"""

    def __init__(self):
        self.line = "";  # the line
        self.mess = "";  # the message type 001, etc
        self.proc = "";  # the process it refers to
        self.date = "";  # dd/mm
        self.hour = "";  # hh
        self.time = "";  # hh:mm
        self.site = "unknown";  # the site, if it was in message
        self.node = "unknown";  # the node, if it was in message
        self.code = "";   # return code
        self.rtim = "";   # run time
        self.memr = 0;   # run time
        self.disk = 0;   # run time
        self.hago = 999;   # hours since this message (for report)



    def __str__(self):
        line = ""
        m = condorCode().mess[self.mess]
        #line += "{0:16s} {1:s} {2:s} {3:s} {4:s} {5:s} {6:s}".format(self.mess,
        line += "{0:3s} {1:16s} {2:5s} {3:5s} {4:2s} {5:s} {6:s} {7:3s}".format(
                 self.mess,m,self.proc,
                 self.date,self.hour,self.time,self.site,self.code)
        if self.mess == "005" :
            line += " {0:5s} {1:5d} {2:5d}".format(
                self.timr,self.disk,self.memr)
        # looks like:
        #005 054 03/22 14 14:30:04 Clemson 65
        return line

    def set(self, m, p, d, h, t, s="", c=""):
        """Set the contents of the message, site and code optional"""
        self.mess = m
        self.proc = p
        self.date = d
        self.hour = h
        self.time = t
        self.site = s
        self.code = c

    def setSite(self, s):
        """Set the site name of the message"""
        self.site = s

    def setNode(self, c):
        """Set the node name of the message"""
        self.node = c

    def setCode(self, c):
        """Set the return code of the message"""
        self.code = c

    def setTimr(self, c):
        """Set the CPU time"""
        self.timr = c

    def setMemr(self, c):
        """Set the memory usage (MB)"""
        self.memr = c

    def setDisk(self, c):
        """Set the disk usage (GB)"""
        self.disk = c


class condorParser:
    """A class which takes a condorLog and prints messages or summaries"""

    def __init__(self):
        self.clear()

    def clear(self):
        self.message = True
        self.summary = False
        self.full = False
        self.jobid = 0
        self.messages = []
        self.log = condorLog()

    def setLog(self, log):
        """setLog(CondorLog)
           Tell the parser what log file to work on"""
        self.clear()
        self.log = log
        self.parse()

    def message(self, q=True):
        """Request to print the condor messages
           By default, skip the Imagine Size and Jobs Ad Updated"""
        self.message = q

    def summary(self, q=True):
        """Request to print the summary of condor messages
           By default, skip the Imagine Size and Jobs Ad Updated"""
        self.summary = q

    def full(self, q=True):
        """Request to print all the condor messages, even boring ones"""
        self.full = q

    def __str__(self):
        #print(type(self.inFile))
        line = ""
        line += "{0:s}\n".format(self.log)
        self.printSummary()
        return line

    def parse(self, log=None):

        self.messages = []

        if log != None:
            self.log = log

        t = self.log.ll[0]
        # pick out the jobid
        t = t.replace('(',' ').split()[1]
        self.jobid = t

        # first pass through the log, just record 
        # all the messages
        n = 0
        mm = condorMsg()
        for t in self.log.ll:
            qm = ('(' in t[:40] and '/' in t[:40] and ':' in t[:40])
            # if this the start of a message block
            if qm :
                # done with previous message, save it
                # after protecting for first line in log file
                if n > 0 :
                    self.messages.append(mm)
                # now start a new message
                t2 = t.replace("."," ")
                ss = t2.split()
                hh = ss[5].split(":")[0]
                mm = condorMsg()
                mm.set(ss[0],ss[2],ss[4],hh,ss[5])
                #print("new mess",n,len(self.messages),t)
                n += 1
            else:
                # see if this message includes a return code
                # or site info
                if "return value" in t:
                    t2 = t.replace(")"," ").split()[5]
                    mm.setCode(t2)
                if "Abnormal termination" in t:
                    t2 = t.replace(")"," ").split()[4]
                    mm.setCode(t2)
                if "Run Remote Usage" in t:
                    t2 = t.replace(","," ").split()[2]
                    mm.setTimr(t2[0:5])
                if "Memory (MB)          :" in t:
                    t2 = t.split()[3]
                    mm.setMemr(int(t2))
                if "Disk (KB)            :" in t:
                    t2 = t.split()[3]
                    mm.setDisk(int(t2)/1000)
                if "JOB_Site" in t:
                    t2 = t.replace('"',' ').split()[2]
                    # don't record unknown sites
                    if "Unknown" not in t2:
                        mm.setSite(t2)
                if "JOB_GLIDEIN_SiteWMS_Slot" in t:
                    t2 = t.replace('"',' ').replace('@',' ').split()[-1]
                    mm.setNode(t2)

        # we have one more current message
        # the if protects against empty log file case
        if n>0 :
            self.messages.append(mm)

        # now for every 028 message (job add updated)
        # associate the site and node to the previous 
        # non-028 message for this process
        for i in xrange(len(self.messages)-1,0,-1):
            if self.messages[i].mess == "028":
                site = self.messages[i].site
                node = self.messages[i].node
                proc = self.messages[i].proc
                # now count backwards and fill sites
                for j in xrange(i-1,-1,-1):
                    if self.messages[j].mess != "000" and \
                                 self.messages[j].proc == proc:
                        self.messages[j].site = site
                        self.messages[j].node = node
                        break

        # for every 012 message (held)
        # try to assign a site because following 028 has no site
        for i in xrange(0,len(self.messages),1):
            if self.messages[i].mess == "012":
                # now count backwards and try to find a site
                for j in xrange(i-1,-1,-1):
                    if self.messages[j].proc == self.messages[i].proc:
                        # if previous event was release, then no site
                        if self.messages[j].mess == "013":
                            break;
                        # if previous message was submitted, the no site
                        if self.messages[j].mess == "000":
                            break;
                        # if previous message had a site, use it
                        if self.messages[j].site != "":
                            self.messages[i].site = self.messages[j].site
                            self.messages[i].node = self.messages[j].node
                            break;
                if self.messages[i].site == "":
                    self.messages[i].site = "unknown"

        # for every 007 message (shadow exception)
        # try to assign a site because there is no following 028
        for i in xrange(0,len(self.messages),1):
            if self.messages[i].mess == "007":
                # now count backwards and try to find a site
                for j in xrange(i-1,-1,-1):
                    if self.messages[j].proc == self.messages[i].proc:
                        # if previous message had a site, use it
                        if self.messages[j].site != "":
                            self.messages[i].site = self.messages[j].site
                            self.messages[i].node = self.messages[j].node
                            break;
                if self.messages[i].site == "":
                    self.messages[i].site = "unknown"

        #print("mesages {0:d} {1:d}".format(n,len(self.messages)))

        return

    def printBasic(self, verbose=1):
        for m in self.messages:
            if m.mess not in ["006","028"]:
                print(m)
        return

    def printJob(self, job=-1):
        for m in self.messages:
            if int(m.proc) == job:
                print(m)
                #if m.mess not in ["006","028"]:
                #    print(m)
        return

    def printSummary(self):
        cc = condorCode().code
        messCounts = {} # count haw many of what kind of message
        rcCounts = {}   # count return codes
        rcCountsSite = []   # count return codes by site
        for m in self.messages:
            k = m.mess
            if messCounts.has_key(k):
                messCounts[k] += 1
            else:
                messCounts[k] = 1

            # if terminate code
            if m.mess == "005":
                k = m.code
                if rcCounts.has_key(k):
                    rcCounts[k] += 1
                else:
                    rcCounts[k] = 1

                # now rc by site
                s = m.site
                found = False
                for i,x in enumerate(rcCountsSite):
                    if x[0]==s and x[1]==k:
                        found = True
                        rcCountsSite[i] = (x[0],x[1],x[2] + 1)
                if not found:
                    rcCountsSite.append( (s,k,1) )
                
                
        print("count, message:")
        for k in messCounts:
            note = ""
            if cc.has_key(k):
                note = cc[k]
            print("{0:6d} {1:s} {2:s}".format(messCounts[k],k,note))
        print("count, return code:")
        for k in rcCounts:
            print("{0:6d} {1:3s}".format(rcCounts[k],k))
        print("count, return code, site:")
        rcCountsSite.sort()
        for x in rcCountsSite:
            print("{0:6d} {1:3s} {2:s}".format(x[2],x[1],x[0]))

        return

    def printTime00(self):

        cc = condorCode().code
        messCounts = {} # count haw many of what kind of message
        rcCounts = {}   # count return codes
        times = []
        for m in self.messages:
            dh = m.date+m.hour
            if not dh in times: 
                times.append(dh)
        print(" date   hr    idle  run   strt  stop  fail  disc  evct")
        rn = 0 # running count of running jobs
        jb = 0 # number of jobs
        ss = 0 # running count of final temrinations
        for hh in times:
            st = 0 # stops this hout (includes redisconnect)
            sp = 0 # stops this hout (includes disconnect and evictions)
            fl = 0 # final temrination with non-zero error
            ds = 0 # disconnects in this hour
            ev = 0 # evictions in this hour
            for m in self.messages:
                dh = m.date+m.hour
                if hh == dh:
                    if m.mess == "000":
                        jb += 1
                    if m.mess == "001":
                        st += 1
                    if m.mess == "013":
                        st += 1
                    if m.mess == "023":
                        st += 1
                    if m.mess == "005":
                        sp += 1
                        ss += 1
                        if m.code != "0":
                            fl += 1
                    if m.mess == "012":
                        sp += 1
                    if m.mess == "022":
                        sp += 1
                        ds += 1
                    if m.mess == "009":
                        sp += 1
                    if m.mess == "004":
                        sp += 1
                        ev += 1
            rn = rn + st - sp
            # there are always odd cases that we don't understand, protect rn
            if rn > (jb-ss) : rn = (jb-ss)
            print(" {0:s}  {1:s} {2:5d} {3:5d} {4:5d} {5:5d} {6:5d} {7:5d} {8:5d}".\
                format(hh[0:5],hh[5:7],jb-rn-ss,rn,st,sp,fl,ds,ev))
        return


    def printTime(self):

        jb = 0
        for m in self.messages:
            if m.mess == "000": jb += 1
        states = [0]*jb
        goodMess = ["000","001","004","005","009","012","013","022","023"]

        m = self.messages[0]  # prime loop
        dh = m.date+m.hour
        i = 0
        n = len(self.messages)
        print(" date   hr   idle   run   stop  fail  disc  evct")
        for m in self.messages:
            if m.mess in goodMess :
                if m.mess == "005":
                    if m.code == "0":
                        states[int(m.proc)] = m.mess # good end
                    else:
                        states[int(m.proc)] = "099"  # error end
                else:
                    states[int(m.proc)] = m.mess
            # test if the hour has passed and need to make a report
            dh2 = "done"
            if i < n-1 :
                m2 = self.messages[i+1]
                dh2 = m2.date+m2.hour
            # report

#        self.code["000"] = "Job submitted from host"
#        self.code["001"] = "Job executing on host"
#        self.code["004"] = "Job was evicted"
#        self.code["005"] = "Job terminated"
#        self.code["012"] = "Job was held"
#        self.code["013"] = "Job was released"
#        self.code["022"] = "Job disconnected, attempting to reconnect"
#        self.code["023"] = "Job reconnected"
#        self.code["024"] = "Job reconnected failed"

            if dh2 != dh:
                n00 = states.count("000")
                n01 = states.count("001")
                n04 = states.count("004")
                n05 = states.count("005")
                n12 = states.count("012")
                n13 = states.count("013")
                n22 = states.count("022")
                n23 = states.count("023")
                n24 = states.count("024")
                n99 = states.count("099")

                jb = len(states) # number of jobs
                rn = n01 + n23 + n22; # running 
                hd = n12  # held
                fl = n99 # final termination with non-zero error
                ss = n05 + fl # final terminations
                ds = n22 # disconnected                
                ev = n04 # evicted
                il = jb-(rn+ss) # idle
               
                print(" {0:s}  {1:s} {2:5d} {3:5d} {4:5d} {5:5d} {6:5d} {7:5d}".\
                format(dh[0:5],dh[5:7],il,rn,ss,fl,ds,ev))
            dh = dh2
            i += 1

        return

    def printFailed(self):
        for m in self.messages:
            if m.mess == "005" and m.code != "0":
                print(m," ",m.node)

        return

    def printNodes(self):
        print("fail, succ, node for nodes with failures")
        nodelist = []
        for m in self.messages:
            if m.mess == "005":
                found = False
                for i,rec in enumerate(nodelist):
                    if rec[2] == m.node:
                        found = True
                        if m.code == "0": rec = (rec[0], rec[1] +1, rec[2])
                        else: rec = (rec[0] + 1, rec[1], rec[2])
                        nodelist[i] = rec
                if not found:
                    if m.code == "0": rec = (0,1,m.node)
                    else: rec = (1,0,m.node)
                    nodelist.append(rec)

        # reverse sort on second item, successes
        nodelist = sorted(nodelist, key=itemgetter(1))
        # now sort on first item, failures
        nodelist = sorted(nodelist, key=itemgetter(0), reverse=True)
        for n in nodelist:
            #if n[0]>0:
            #    print("{0:3d} {1:3d} {2:s}".format(*n))
            print("{0:3d} {1:3d} {2:s}".format(*n))

        return



##################################
#
# classes for conMonReport
#
#################################

class timeUtil:
    """Some time conversion utilties"""
    def __init__(self):
        # now as a struct
        #self.nstr = time.localtime(1478050572.0) # 1478048400.0
        self.nstr = time.localtime()
        # now as epoch sec
        self.nsec = time.mktime(self.nstr)
        # top of the hour as epoch sec
        self.hsec = self.nsec - self.nstr.tm_min*60 - self.nstr.tm_sec

        # compute best guess year for each month
        # since condor doesn't tell you
        ny = int(self.nstr.tm_year)%100   # now year as 2-digit int
        nm = int(self.nstr.tm_mon)    # now month as int
        # store result as a conversion between 2-digit month string 
        # and 2-digit year string
        self.cyearc = []
        # force index to equal month number by filling index=0
        self.cyearc.append("zero")
        for im in range(1,13):
            # if month is late in year and now is early in year, guess last year
            if im > 6 and nm < 6 :
                self.cyearc.insert(im+1,"{0:02d}".format(ny-1))
            else:
                self.cyearc.insert(im+1,"{0:02d}".format(ny))

    def hoursAgo(self, monthdate, timestr):
        # convert mm/dd hh:mm:ss to rounded hours ago

        yy = self.cyearc[int(monthdate[0:2])]
        # time as a struct
        tstr = time.strptime(yy+"/"+monthdate+" "+timestr,"%y/%m/%d %H:%M:%S")
        # time as epoch sec
        tsec = time.mktime(tstr)
        hoursAgo = int(math.floor( (self.hsec-tsec)/3600.0 + 1.0))
        return hoursAgo

    def hoursAgoString(self, hoursAgo):
        # convert int hours ago to "mm/dd hh"
        t = self.nsec - 3600 * hoursAgo
        # time as struct
        tstr = time.localtime(t)
        # print(format)
        tstring = time.strftime("%m/%d %H", tstr)
        return tstring


class condorParsedLog:
    """A utility class containing all lines of one condor 
log file, after parsing"""

    def __init__(self, inFileName=""):
        self.inFile = inFileName;
        self.ll = []
        self.messages = []
        if len(inFileName) >0 :
            self.read(inFileName)
        # this is a measure of the jobs in this site and 
        # will determine the order it is printed
        self.size = 0 

    def __str__(self):
        line = ""
        line += "{0:d} lines in {1:s}".format(len(self.ll),self.inFile)
        return line

    def read(self, inFileName=""):
        if len(inFileName) > 0:
            self.inFile = inFileName;
        try:
            f = open(self.inFile, 'r')
            self.ll = f.read().splitlines()
            f.close()
        except:
            print("\nError - could not read log file {0:s}!\n".\
                format(self.inFile))
            raise
        # convert input lines to condorMsg
        for l in self.ll:
            m = condorMsg()
            s = l.split()
            m.set(s[0],s[2],s[3],s[4],s[5])
            if len(s) >=  7 : m.setSite(s[6])
            if len(s) >=  8 : m.setCode(s[7])
            if len(s) >=  9 : m.setTimr(s[8])
            if len(s) >= 10 : m.setMemr(s[9])
            if len(s) >= 11 : m.setDisk(s[10])
            self.messages.append(m)
        #print(self)


class conMonLine:
    """One line of conMon summary"""

    def __init__(self):
        self.clear()

    def clear(self):
        # the date for the line
        self.date = ""
        self.hour = ""
        # these are current states of jobs
        self.run = 0
        self.idle = 0
        self.hold = 0
        # these are counts of things that happened in this hour
        self.start = 0
        self.success = 0
        self.failed = 0
        self.disc = 0
        self.error = 0
        self.evict = 0
        self.shadow = 0

    def __str__(self):
        line = ""
        line += "{0:4s} {1:2s} {2:5d} {3:5d} {4:5d}   {5:4d} {6:4d} {7:4d} {8:4d} {9:4d} {10:4d} {11:4d}".format( \
        self.date,self.hour, \
        self.run,self.idle,self.hold, \
        self.start,self.success,self.failed, \
        self.disc,self.error,self.evict,self.shadow )

        return line


class conMonReport:
    """A class which takes a set of parsed condor logs and makes an html summary """

    def __init__(self):
        self.clear()
        self.timeu = timeUtil()

    def clear(self):
        self.outHtml = ""
        self.timeLimit = 48
        # list of condorParsedLog 
        self.plogs = []
        self.report = {}
        self.report["all"] = []

    def setHtml(self, html):
        """setHtml(string)
           set the file to write the output"""
        self.outHtml = html

    def setTimeLimit(self, limit):
        """setTimeLimit(limit)
           set the limit on how far back to summarize (hours)"""
        self.timeLimit = limit

    def addLog(self, log):
        """addLog(string)
           Using its filespec, add a parsed log file to the list"""
        plog = condorParsedLog(log)
        self.plogs.append(plog)

    def run(self):
        """Create and write the html summary"""
        if len(self.plogs) <=0:
            return

        for plog in self.plogs:
            self.sumLog(plog)


    def sumLog(self, plog):
        """add a processed log to the sums"""

        # needed for translations
        cc = condorCode()

        for m in plog.messages:
            # make sure we an entry for all sites
            if m.site != "" :
                if not m.site in self.report:
                    self.report[m.site] = []
            # fill hours ago field of messages
            m.hago = self.timeu.hoursAgo(m.date,m.time)

        # how many jobs are in this log
        nj = 0
        for m in plog.messages:
            if int(m.proc) > nj : nj = int(m.proc)
        # the processes numbers start with 0
        nj = nj + 1

        # create new conMonLine for reach hour and site
        for site in self.report:
            if len(self.report[site]) == 0 :
               for h in xrange(0,self.timeLimit+1) :
                   self.report[site].append(conMonLine())
        
        # create a state record for each job
        states = ["notstarted"]*nj
        stateSites = [""]*nj

        # start in the first hour, 
        # roll forward through log, update states and count events
        for im in range(len(plog.messages)) :
            cm = plog.messages[im]
            hh = cm.hago
            # if this is in interesting range
            if hh<=self.timeLimit and hh>=0:
                # if this is a specific countable event
                if cm.mess == "001" :
                    self.report["all"][hh].start = \
                        self.report["all"][hh].start + 1
                    if cm.site != "":
                        self.report[cm.site][hh].start = \
                            self.report[cm.site][hh].start + 1
                # end success
                if cm.mess == "005" and cm.code == "0":
                    self.report["all"][hh].success = \
                        self.report["all"][hh].success + 1
                    if cm.site != "":
                        self.report[cm.site][hh].success = \
                            self.report[cm.site][hh].success + 1
                # end failure or abort
                if (cm.mess == "005" and cm.code != "0") or cm.mess == "009":
                    self.report["all"][hh].failed = \
                        self.report["all"][hh].failed + 1
                    if cm.site != "":
                        self.report[cm.site][hh].failed = \
                            self.report[cm.site][hh].failed + 1
                # disconnect
                if cm.mess == "022":
                    self.report["all"][hh].disc = \
                        self.report["all"][hh].disc + 1
                    if cm.site != "":
                        self.report[cm.site][hh].disc = \
                            self.report[cm.site][hh].disc + 1
                # evict
                if cm.mess == "004":
                    self.report["all"][hh].evict = \
                        self.report["all"][hh].evict + 1
                    if cm.site != "":
                        self.report[cm.site][hh].evict = \
                            self.report[cm.site][hh].evict + 1
                # shadow
                if cm.mess == "007":
                    self.report["all"][hh].shadow = \
                        self.report["all"][hh].shadow + 1
                    if cm.site != "":
                        self.report[cm.site][hh].shadow = \
                            self.report[cm.site][hh].shadow + 1
                # errors, not sure of the state
                if (cm.mess == "015" or cm.mess == "016" \
                        or cm.mess == "018" or cm.mess == "021"):
                    self.report["all"][hh].error = \
                        self.report["all"][hh].error + 1
                    if cm.site != "":
                        self.report[cm.site][hh].error = \
                            self.report[cm.site][hh].error + 1
                    
            # analyze states for each job
            j = int(cm.proc)
            if cc.state[cm.mess] !="noop" :
                states[j] = cc.state[cm.mess]
                stateSites[j] = cm.site

            # if the next hour is  new hour or the end, then write states
            if (hh>=0 and hh<=self.timeLimit) and \
                (im>=len(plog.messages)-1 or \
                    plog.messages[im+1].hago != hh) :
                if im<len(plog.messages)-1 :   # if not end of file
                    hhlim = plog.messages[im+1].hago  # loop over missing
                else:
                    # this is file end, copy the states forward 
                    # all the way to now
                    hhlim = -1
                for j in range(nj):
                    for hhi in range(hh,hhlim,-1):
                        if states[j]=="executing":
                            self.report["all"][hhi].run = \
                                self.report["all"][hhi].run + 1
                            if stateSites[j] != "":
                                self.report[stateSites[j]][hhi].run = \
                                    self.report[stateSites[j]][hhi].run + 1
                        if states[j]=="idle":
                            self.report["all"][hhi].idle = \
                                self.report["all"][hhi].idle + 1
                            if stateSites[j] != "":
                                self.report[stateSites[j]][hhi].idle = \
                                    self.report[stateSites[j]][hhi].idle + 1
                        if states[j]=="held":
                            self.report["all"][hhi].hold = \
                                self.report["all"][hhi].hold + 1
                            if stateSites[j] != "":
                                self.report[stateSites[j]][hhi].hold = \
                                    self.report[stateSites[j]][hhi].hold + 1
                        

    def write(self):
        """write()
           write the results to sysout or the html file"""

        fn=sys.stdout
        if self.outHtml != "":
            fn = open(self.outHtml,"w")
            fn.write("<pre>\n\n")

        # determine a size for each site, to print them in order
        sitesort = []
        for site in self.report:
            size = 0
            for i in range(0,min(12,self.timeLimit)):
                size = size + self.report[site][i].run
            # force "all" to be first
            if site == "all" :
                size = 999999
            sitesort.append((size,site))

        sitesort.sort(None,None,True)

        for ntp in sitesort:
            site = ntp[1]
            headerStr = "\n"+site+":\n"+\
            "           run  idle  hold   strt succ fail disc  err evct shdw\n"
            fn.write(headerStr)
            ha = 0
            for line in self.report[site]:
                temp = self.timeu.hoursAgoString(ha)
                line.date = temp.split()[0]
                line.hour = temp.split()[1]
                if ha==0 : line.hour="-"
                fn.write(str(line)+"\n")
                ha = ha +1

        if self.outHtml != "":
            fn.write("</pre>")
            fn.close()

