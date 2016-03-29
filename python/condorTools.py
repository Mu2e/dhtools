
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
            print "\nError - could not read log file {0:s}!\n".\
                format(self.inFile)
            raise

class condorMsg:
    """A utility class containing one condor message"""

    def __init__(self):
        self.line = "";  # the line
        self.mess = "";  # the message type 001, etc
        self.proc = "";  # the process it refers to
        self.date = "";  # dd/mm/yy
        self.hour = "";  # hh
        self.time = "";  # hh:mm
        self.site = "unknown";  # the site, if it was in message
        self.node = "unknown";  # the node, if it was in message
        self.code = "";   # return code

    def __str__(self):
        line = ""
        line += "{0:s} {1:s} {2:s} {3:s} {4:s} {5:s} {6:s}".format(self.mess,
                 self.proc,self.date,self.hour,self.time,self.site,self.code)
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

    def setCode(self, c):
        """Set the return code of the message"""
        self.code = c

    def setNode(self, c):
        """Set the node name of the message"""
        self.node = c

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
        #print type(self.inFile)
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
            qm = ('(' in t and '/' in t and ':' in t)
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
                #print "new mess",n,len(self.messages),t
                n += 1
            else:
                # see if this message includes a return code
                # or site info
                if "return value" in t:
                    t2 = t.replace(")"," ").split()[5]
                    mm.setCode(t2)
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
                for j in xrange(i-1,-1,-1):
                    if self.messages[j].mess != "000" and \
                                 self.messages[j].proc == proc:
                        self.messages[j].site = site
                        self.messages[j].node = node
                        break

        #print "mesages {0:d} {1:d}".format(n,len(self.messages))

        return

    def printBasic(self, verbose=1):
        for m in self.messages:
            if m.mess not in ["006","028"]:
                print m
        return

    def printJob(self, job=-1):
        for m in self.messages:
            if int(m.proc) == job:
                print m
                #if m.mess not in ["006","028"]:
                #    print m
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
                
                
        print "count, message:"
        for k in messCounts:
            note = ""
            if cc.has_key(k):
                note = cc[k]
            print "{0:6d} {1:s} {2:s}".format(messCounts[k],k,note)
        print "count, return code:"
        for k in rcCounts:
            print "{0:6d} {1:3s}".format(rcCounts[k],k)
        print "count, return code, site:"
        rcCountsSite.sort()
        for x in rcCountsSite:
            print "{0:6d} {1:3s} {2:s}".format(x[2],x[1],x[0])

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
        print " date   hr    idle  run   strt  stop  fail  disc  evct"
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
            print " {0:s}  {1:s} {2:5d} {3:5d} {4:5d} {5:5d} {6:5d} {7:5d} {8:5d}".\
                format(hh[0:5],hh[5:7],jb-rn-ss,rn,st,sp,fl,ds,ev)
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
        print " date   hr   idle   run   stop  fail  disc  evct"
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
               
                print " {0:s}  {1:s} {2:5d} {3:5d} {4:5d} {5:5d} {6:5d} {7:5d}".\
                format(dh[0:5],dh[5:7],il,rn,ss,fl,ds,ev)
            dh = dh2
            i += 1

        return

    def printFailed(self):
        for m in self.messages:
            if m.mess == "005" and m.code != "0":
                print m

        return

    def printNodes(self):
        print "fail, succ, node for nodes with failures"
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
                    if m.code == "0": rec = (1,0,m.node)
                    else: rec = (0,1,m.node)
                    nodelist.append(rec)
        for n in nodelist:
            if n[0]>0:
                print "{0:3d} {1:3d} {2:s}".format(*n)

        return
